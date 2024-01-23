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

import com.google.common.util.concurrent.ThreadFactoryBuilder

import java.util.{ArrayList, List}
import java.util.concurrent.{Executors, Future, ThreadPoolExecutor}
import com.gurobi.gurobi.{GRB, GRBEnv, GRBLinExpr, GRBModel, GRBVar}

import scala.collection.mutable.{HashMap, HashSet}
import org.apache.thrift.TProcessor
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.{TServer, TSimpleServer}
import org.apache.thrift.transport.{TServerSocket, TSocket, TTransport}

import org.apache.spark._
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.internal.config.{EARLY_SCHEDULE_ENABLE, RESCHEDULE_TRIGGER_RATIO, SITE_NUMBER, THRIFT_SERVER_HOST}
import org.apache.spark.rpc.{IsolatedRpcEndpoint, RpcEnv}
import org.apache.spark.scheduler.EarlyScheduleTracker.ENDPOINT_NAME
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{LoadReport, ModelReady, SiteBandwidthUpdate}
import org.apache.spark.scheduler.rpc.{Request1, Request3, Request4, Response1, ServiceHandler, gurobiService}
import org.apache.spark.util.{UninterruptibleThread, Utils}


private[spark] class EarlyScheduleTracker(sc: SparkContext, conf: SparkConf, rpcEnv: RpcEnv,
                                          appUniqueId: String)
  extends Logging {

  val enableEarlySchedule = conf.get(EARLY_SCHEDULE_ENABLE)

  val earlyScheduleTrackerEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
                                                          new EarlySchedulerEndpoint())

  var transport: TTransport = new TSocket(conf.get(THRIFT_SERVER_HOST), 7777)  // Python Server
  val server = launchThriftServer()
  val thriftClient = buildThriftClient()

  val siteSlots = new HashMap[String, Int]
  val executorCores = new HashMap[String, Int]
  val hostToExecutors = new HashMap[String, HashSet[String]]

  private val siteLocation = HashMap(
//    "0" -> Seq("10.176.24.55", "10.176.24.56", "10.176.24.57").map(TaskLocation(_)),
//    "1" -> Seq("10.176.24.58", "10.176.24.59", "10.176.24.60").map(TaskLocation(_))
    "0" -> Seq("analysis-5"),
    "1" -> Seq("analysis-6"),
    "2" -> Seq("analysis-7"),
    "3" -> Seq("analysis-3"),
    "4" -> Seq("analysis-4"),
  )
  private val hostSite: HashMap[String, String] = HashMap(
    //      "10.176.24.55" -> "0",
    //      "10.176.24.56" -> "0",
    //      "10.176.24.57" -> "0",
    //      "10.176.24.58" -> "1",
    //      "10.176.24.59" -> "1",
    //      "10.176.24.60" -> "1"
    "10.176.24.55" -> "0",
    "10.176.24.56" -> "1",
    "10.176.24.57" -> "2",
    "10.176.24.53" -> "3",
    "10.176.24.54" -> "4",
    "analysis-5" -> "0",
    "analysis-6" -> "1",
    "analysis-7" -> "2",
    "analysis-3" -> "3",
    "analysis-4" -> "4",
  )

  val shuffleManager = SparkEnv.get.shuffleManager
  val shuffleManagerName = conf.get(config.SHUFFLE_MANAGER)
  val updatePartitionSiteMethod =
    if (shuffleManagerName.equals("org.apache.spark.shuffle.celeborn.SparkShuffleManager")) {
      Utils.classForName(shuffleManagerName).getDeclaredMethod("updatePartitionSite",
        classOf[String], classOf[Integer], classOf[Array[Integer]])
    } else {
      null
    }

  private val numSites: Int = conf.get(SITE_NUMBER)

  private val bandwidthMatrix = initBandwidthMatrix()

  private val loadWeight = (0.4, 0.3, 0.2, 0.1)

  // (hostUsageMap, siteLoadMap)
  type LoadStatus = (HashMap[String, java.lang.Double], HashMap[String, java.lang.Double])
  private val loadStatus: LoadStatus = initLoadStatus()

  private val rescheduleTriggerRatio = conf.get(RESCHEDULE_TRIGGER_RATIO)

  // <modelUniqueId, (a,b)>
  private val stageToM2 = new HashMap[String, Array[Double]]

  private val earlyScheduleDecision = new HashMap[String, Int]

  private val threadPool = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("Early-Scheduler-Ask-Load-%d")
      .setThreadFactory((r: Runnable) => new UninterruptibleThread(r, "unused"))
      .build()
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  def getEarlyScheduleDecision(stageId: Int, taskId: Int): String = {
    if (stageId == -1 || !enableEarlySchedule) {
      return null
    }

    var scheduleSiteIdx = earlyScheduleDecision.getOrElse(s"${stageId}_${taskId}", -1)
//    if (stageId == 2) {
//      "0"
//    } else if (stageId == 1) {
//      "1"
//    } else {
//      null
//    }
    // TODO: set larger value for high complexity calculation
    var time = 5
    if (scheduleSiteIdx == -1 && time > 0) {
      // wait for making decision
      Thread.sleep(1000)
      scheduleSiteIdx = earlyScheduleDecision.getOrElse(s"${stageId}_${taskId}", -1)
      time -= 1
    }
    logInfo(s"[Early-Schedule] Ask for stage: ${stageId}, task: ${taskId}, res: ${scheduleSiteIdx}")

    if (scheduleSiteIdx == -1) {
      logError(s"[Early-Schedule] Do not make any schedule decision after 5s wait")
      scheduleSiteIdx = 0
    }
    String.valueOf(scheduleSiteIdx)
  }

  private def launchThriftServer(): TServer = {
    if (!enableEarlySchedule) {
      return null
    }
    val tprocessor: TProcessor = new gurobiService.Processor(new ServiceHandler(this))
    val serverTransport: TServerSocket = new TServerSocket(9898)
    val server: TServer = new TSimpleServer(new TServer.Args(serverTransport).processor(tprocessor))
    val serverThread: Thread = new Thread(() => server.serve)
    serverThread.setDaemon(true)
    serverThread.start()
    server
  }

  private def buildThriftClient(): gurobiService.Client = {
    if (!enableEarlySchedule) {
      return null
    }
    transport.open()
    val protocol = new TBinaryProtocol(transport)
    val client = new gurobiService.Client(protocol)
    client.registerApplication(new Request3(appUniqueId, Utils.localCanonicalHostName(), 9898))
    client
  }

  private def initLoadStatus(): LoadStatus = {
    val hostUsageMap: HashMap[String, java.lang.Double] = HashMap()
    val siteLoadMap: HashMap[String, java.lang.Double] = HashMap()
    hostSite.keys.foreach(hostUsageMap.put(_, 0))
    siteLocation.keys.foreach(siteLoadMap.put(_, 0))

    new LoadStatus(hostUsageMap, siteLoadMap)
  }

  private def initBandwidthMatrix(): List[List[Integer]] = {
    val bandwidthMatrix: List[List[Integer]] = new ArrayList()
    for (i <- 0 until numSites) {
      val siteBandwidth: List[Integer] = new ArrayList()
      for (j <- 0 until numSites) {
        siteBandwidth.add(0)
      }
      siteBandwidth.set(i, 10001)
      bandwidthMatrix.add(siteBandwidth)
    }

    bandwidthMatrix
  }

  def updateSiteSlots(host: String, executorId: String, allCores: Int): Unit = {
    if (!hostToExecutors.contains(host)) {
      hostToExecutors(host) = new HashSet[String]()
    }
    hostToExecutors(host).add(executorId)

    var newSlots = allCores
    if (executorCores.contains(executorId)) {
      newSlots = allCores - executorCores(executorId)
    }
    executorCores(executorId) = allCores
    val site: String = hostSite.getOrElse(host, "-1")
    val currentSlots = siteSlots.getOrElse(site, 0)
    siteSlots(hostSite(host)) = currentSlots + newSlots

    // remove executor
    if (allCores == 0) {
      hostToExecutors(host).remove(executorId)
      executorCores.remove(executorId)
    }
  }

  def stop(): Unit = {
    if (!enableEarlySchedule) {
      return
    }
    thriftClient.deregisterApplication(new Request4(appUniqueId))
    transport.close()
    server.stop()
  }

  def makeScheduleDecision(mapStageId: Int, numMappers: Int, numReducers: Int,
                           shuffleId: Int): Unit = {
    val (decisions, time, linkTime, u) = makeInternalDecision(mapStageId, numMappers, numReducers)
    logInfo(s"Make new schedule decision: $decisions, broadcast to every Site Master")
    for (idx <- 0 until numReducers) {
      val scheduleUniqueId = s"${mapStageId}_${idx}"
      earlyScheduleDecision.update(scheduleUniqueId, decisions.get(idx))
    }
    logInfo(s"current decision map: ${earlyScheduleDecision}")
    if (updatePartitionSiteMethod != null) {
      val decisionArr: Array[Integer] = decisions.toArray(Array.ofDim[Integer](decisions.size))
      updatePartitionSiteMethod.invoke(shuffleManager, appUniqueId,
                                       new Integer(shuffleId), decisionArr)
    }
  }

  /**
   * make network-aware task placement decision
   * @return
   */
  private def makeInternalDecision(mapStageId: Int, numMappers: Int, numReducers: Int)
                            : (List[Integer], Double, List[List[java.lang.Double]], Double) = {
    // NOTE: replace with CBO
    // S*M
//    val finishTime = m1(mapStageId)
    // S*R*M
//    val outputSize = m2(mapStageId, reduceStageId)
    // val responseM2 = thriftClient.m2(new Request7(appUniqueId, mapStageId))
    // val outputSize = responseM2.outputSize

    val siteLoads = new Array[Double](siteSlots.size)
    val futureMap = new HashMap[String, Future[_]]
    siteSlots.foreach(item => {
      val site = item._1
      val execSet = hostToExecutors(siteLocation(site).head)
      val it = execSet.iterator
      val executorId = it.next()
      val future: Future[_] = threadPool.submit(new Runnable {
        override def run(): Unit = {
          val siteLoad = sc.schedulerBackend.askRecentLoad(executorId)
          siteLoads(Integer.parseInt(site)) = siteLoad
          logInfo(s"[Early-Schedule] Site $site Load: $siteLoad")
        }
      })
      futureMap.put(site, future)
    })
    futureMap.foreach(item => {
      item._2.get()
    })

    var loadMin: Double = 10
    var loadMax: Double = -1
    for (i <- 0 until siteSlots.size) {
      loadMin = Math.min(loadMin, siteLoads(i))
      loadMax = Math.max(loadMax, siteLoads(i))
    }
    val capacity = new Array[java.lang.Double](siteSlots.size)
    for (i <- 0 until siteSlots.size) {
      val c = siteSlots(String.valueOf(i)) /
        (0.5 + 0.5 * (siteLoads(i) - loadMin) / (loadMax - loadMin))
      capacity(i) = c
      logInfo(s"[Early-Schedule] Site $i , slot:${siteSlots(String.valueOf(i))}, Capacitity:$c")
    }

    val request = new Request1(numMappers, numReducers, numSites, 0.5)
    // NOTE: Comment the following parameters for Test
//    request.setBandwidth(bandwidthMatrix)
//    request.setFinishTime(finishTime)
//    request.setOutputSize(outputSize)
    import scala.collection.JavaConverters._
    request.setSiteLoad(capacity.toList.asJava)
    val response: Response1 = thriftClient.gurobi(request)

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
  private def m1(mapStageId: Int): List[List[Integer]] = {
    null
  }

  /**
   * Mock RPC for model m2
   * @param mapStageId
   * @return size: [S*R*M]
   */
  private def m2(mapStageId: Int, reduceStageId: Int): List[List[List[Integer]]] = {
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
        makeScheduleDecision(mapStageId, numMappers, numReducers, shuffleId)

      case SiteBandwidthUpdate(srcSite, dstSite, bandwidth) =>
        val oriBandwidth = bandwidthMatrix.get(srcSite).get(dstSite)
        val newBandwidth = bandwidth
        bandwidthMatrix.get(srcSite).set(dstSite, newBandwidth)
        if (Math.abs(newBandwidth - oriBandwidth) / oriBandwidth > rescheduleTriggerRatio) {
          // TODO: Dynamic bandwidth handler, such a mess now!
        }

      case LoadReport(host, uCPU, uMem, uBandwidth, uDisk) =>
        val site = hostSite(host)
        val numSiteHosts: Int = siteLocation(site).size
        val usage: java.lang.Double = loadWeight._1 * uCPU + loadWeight._2 * uMem +
                                      loadWeight._3 * uBandwidth + loadWeight._4 * uDisk
        val lastUsage: java.lang.Double = loadStatus._1(host)
        val newSiteLoad = loadStatus._2(site) + (usage - lastUsage) / numSiteHosts
        loadStatus._1(host) = usage
        loadStatus._2(site) = newSiteLoad
//        print(s"$loadStatus\n")
    }
  }
}

private[spark] object EarlyScheduleTracker {
  val ENDPOINT_NAME = "EarlyScheduleTracker"
}