/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.util.{ArrayList, Arrays, List}

import scala.collection.mutable.HashMap

import com.gurobi.gurobi.{GRB, GRBEnv, GRBLinExpr, GRBModel, GRBVar}

import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TSocket, TTransport}

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config.{RESCHEDULE_TRIGGER_RATIO, SITE_NUMBER, THRIFT_SERVER_HOST}
import org.apache.spark.rpc.{IsolatedRpcEndpoint, RpcEnv}
import org.apache.spark.scheduler.EarlyScheduleTracker.ENDPOINT_NAME
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{ModelReady, SiteBandwidthUpdate}
import org.apache.spark.scheduler.rpc.{gurobiService, Request, Response}
import org.apache.spark.util.Utils

private[spark] class EarlyScheduleTracker(sc: SparkContext, conf: SparkConf, rpcEnv: RpcEnv)
  extends Logging {

  val earlyScheduleTrackerEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
                                                          new EarlySchedulerEndpoint())

  var transport: TTransport = new TSocket(conf.get(THRIFT_SERVER_HOST), 7777)  // Python Server
  val thriftClient = buildThriftClient()

  val siteLocation = Map(
    "0" -> Seq("10.176.24.55", "10.176.24.56", "10.176.24.57").map(TaskLocation(_)),
    "1" -> Seq("10.176.24.58", "10.176.24.59", "10.176.24.60").map(TaskLocation(_))
  )

  val shuffleManager = SparkEnv.get.shuffleManager
  val shuffleManagerName = conf.get(config.SHUFFLE_MANAGER)
  val updatePartitionSiteMethod =
    if (shuffleManagerName.equals("org.apache.spark.shuffle.celeborn.SparkShuffleManager")) {
      Utils.classForName(shuffleManagerName).getDeclaredMethod("updatePartitionSite",
        classOf[String], classOf[Int], classOf[Array[String]])
    } else {
      null
    }

  val appUniqueId = sc.applicationAttemptId.map(id => sc.applicationId + "_" + id)
                                           .getOrElse(() => sc.applicationId)

  private val numSites: Int = conf.get(SITE_NUMBER)

  private val bandwidthMatrix = initBandwidthMatrix()

  private val siteLoad: List[java.lang.Double] = new ArrayList[java.lang.Double]()

  private val rescheduleTriggerRatio = conf.get(RESCHEDULE_TRIGGER_RATIO)

  // <modelUniqueId, (a,b)>
  private val stageToM2 = new HashMap[String, Array[Double]]

  private val earlyScheduleDecision = new HashMap[String, Int]

  def getEarlyScheduleDecision(stageId: String, taskId: Int): String = {
    val scheduleSiteIdx = earlyScheduleDecision.getOrElse(s"${stageId}_${taskId}", -1)
    if (scheduleSiteIdx != -1) {
      String.valueOf(scheduleSiteIdx)
    } else {
      null
    }
  }

  private def buildThriftClient(): gurobiService.Client = {
    transport.open()
    val protocol = new TBinaryProtocol(transport)
    val client = new gurobiService.Client(protocol)

    client
  }

  private def initBandwidthMatrix(): List[List[Integer]] = {
    val bandwidthMatrix: List[List[Integer]] = new ArrayList()
    for (i <- 0 until numSites) {
      val siteBandwidth = Arrays.asList(new Integer(numSites))
      siteBandwidth.set(i, 10001)
      bandwidthMatrix.add(siteBandwidth)
    }

    bandwidthMatrix
  }

  def stop(): Unit = {
    transport.close()
  }

  def makeScheduleDecision(mapStageId: String, reduceStageId: String,
                           numMappers: Int, numReducers: Int, shuffleId: Int): Unit = {
    val (decisions, time, linkTime, u) = makeDecision(mapStageId, reduceStageId,
                                                      numMappers, numReducers)

    for (idx <- 0 until numReducers) {
      val scheduleUniqueId = s"${reduceStageId}_${idx}"
      earlyScheduleDecision.update(scheduleUniqueId, decisions.get(idx))
    }
    logInfo(s"Make new schedule decision: $decisions, broadcast to every Site Master")
    if (updatePartitionSiteMethod != null) {
      val result = updatePartitionSiteMethod.invoke(shuffleManager, appUniqueId,
        shuffleId.asInstanceOf[Object], decisions)
    }
  }

  /**
   * make network-aware task placement decision
   * @return
   */
  private def makeDecision(mapStageId: String, reduceStageId: String,
                           numMappers: Int, numReducers: Int)
                            : (List[Integer], Double, List[List[java.lang.Double]], Double) = {
    // S*M
    val finishTime = m1(mapStageId)
    // S*R*M
    val outputSize = m2(mapStageId, reduceStageId)
    val request = new Request(numMappers, numReducers, numSites, 0.5)
    request.setBandwidth(bandwidthMatrix)
    request.setFinishTime(finishTime)
    request.setOutputSize(outputSize)
    request.setSiteLoad(siteLoad)
    val response: Response = thriftClient.gurobi(request)

    val decisions = response.decision
    val time = response.maxTime
    val linkTime = response.linkTime
    val u = response.u
    (decisions, time, linkTime, u)
  }

  // Deprecated
  private def gurobi(numReducers: Int, finishTime: Array[Array[Double]],
                     outputSize: Array[Array[Array[Double]]]) = {
    val env = new GRBEnv
    //    env.set("logFile", "gurobi.log")
    val model = new GRBModel(env)
    val matrix = Array.ofDim[GRBVar](numReducers, numSites)
    for (r <- 0 until numReducers) {
      for (s <- 0 until numSites) {
        matrix(r)(s) = model.addVar(0, 1, 0.0, GRB.CONTINUOUS, s"${r}_${s}")
      }
    }

    for (r <- 0 until numReducers) {
      val rowSumExpr: GRBLinExpr = new GRBLinExpr()
      for (s <- 0 until numSites) {
        rowSumExpr.addTerm(1.0, matrix(r)(s))
      }
      model.addConstr(rowSumExpr, GRB.EQUAL, 1.0, s"row_sum_${r + 1}")
    }

    val maxValue = model.addVar(0.0, Double.MaxValue, 0.0, GRB.CONTINUOUS, "max_value")

    val p = Array.ofDim[Double](numSites, numSites)
    for (j <- 0 until numSites) {
      for (s <- 0 until numSites) {
        for (m <- (0 until numSites).reverse) {
          for (r <- 0 until numReducers) {
            p(j)(s) += 1 // outputSize(j)(r)(m) * matrix(r)(s)
          }
          model.addConstr(maxValue, GRB.GREATER_EQUAL,
            finishTime(j)(m) + p(j)(s) / bandwidthMatrix.get(j).get(s), s"s_m")
        }
      }
    }
    val finalDecisionExpr: GRBLinExpr = new GRBLinExpr()
    finalDecisionExpr.addTerm(1, maxValue)
    model.setObjective(finalDecisionExpr, GRB.MINIMIZE)

    model.optimize()

    if (model.get(GRB.IntAttr.Status) == GRB.OPTIMAL) {
      print("Optimal solution found:")
      val matrix_value: Array[Array[Double]] = matrix.map(row => row.map(_.get(GRB.DoubleAttr.X)))
      val max_value_value: Double = maxValue.get(GRB.DoubleAttr.X)
      print(s"Max Value: $max_value_value")
    }

    model.dispose()
    env.dispose()
  }

  /**
   * Mock RPC for model m1
   * @param mapStageId
   * @return size: [S*M]
   */
  private def m1(mapStageId: String): List[List[Integer]] = {
    null
  }

  /**
   * Mock RPC for model m2
   * @param mapStageId
   * @return size: [S*R*M]
   */
  private def m2(mapStageId: String, reduceStageId: String): List[List[List[Integer]]] = {
    null
  }

  class EarlySchedulerEndpoint extends IsolatedRpcEndpoint with Logging {
    /**
     * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
     */
    override val rpcEnv: RpcEnv = EarlyScheduleTracker.this.rpcEnv

    override def receive: PartialFunction[Any, Unit] = {
      // shuffleId can be fetched in `Dependency`
      case ModelReady(mapStageId, reduceStageId, numMappers, numReducers, shuffleId) =>
        makeScheduleDecision(mapStageId, reduceStageId, numMappers, numReducers, shuffleId)

      case SiteBandwidthUpdate(srcSite, dstSite, bandwidth) =>
        val oriBandwidth = bandwidthMatrix.get(srcSite).get(dstSite)
        val newBandwidth = bandwidth
        bandwidthMatrix.get(srcSite).set(dstSite, newBandwidth)
        if (Math.abs(newBandwidth - oriBandwidth) / oriBandwidth > rescheduleTriggerRatio) {
          // TODO: Dynamic bandwidth handler, such a mess now!
        }

    }
  }
}

private[spark] object EarlyScheduleTracker {
  val ENDPOINT_NAME = "EarlyScheduleTracker"
}