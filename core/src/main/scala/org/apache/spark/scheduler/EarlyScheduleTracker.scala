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

import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.internal.config.RESCHEDULE_TRIGGER_RATIO
import org.apache.spark.rpc.{IsolatedRpcEndpoint, RpcEnv}
import org.apache.spark.scheduler.EarlyScheduleTracker.ENDPOINT_NAME
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{ModelReady, SiteBandwidthUpdate}
import org.apache.spark.util.Utils

private[spark] class EarlyScheduleTracker(sc: SparkContext, conf: SparkConf, rpcEnv: RpcEnv)
  extends Logging {

  val earlyScheduleTrackerEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
                                                          new EarlySchedulerEndpoint())

  val siteLocation = Map(
    "0" -> Seq("10.176.24.55", "10.176.24.56", "10.176.24.57").map(TaskLocation(_)),
    "1" -> Seq("10.176.24.58", "10.176.24.59", "10.176.24.60").map(TaskLocation(_))
  )

  val shuffleManager = SparkEnv.get.shuffleManager
  val shuffleManagerName = conf.get(config.SHUFFLE_MANAGER)
  val updatePartitionSiteMethod =
    if (shuffleManagerName.equals("org.apache.spark.shuffle.celeborn.SparkShuffleManager"))
      Utils.classForName(shuffleManagerName).getDeclaredMethod("updatePartitionSite",
        classOf[String], classOf[Int], classOf[Array[String]])
    else
      null

  val appUniqueId = sc.applicationAttemptId.map(id => sc.applicationId + "_" + id)
                                           .getOrElse(() => sc.applicationId)

  private val siteNum = 5
  private val bandwidthMatrix = Array.ofDim[Double](siteNum, siteNum)
  private val rescheduleTriggerRatio = conf.get(RESCHEDULE_TRIGGER_RATIO)

  // <modelUniqueId, (a,b)>
  private val stageToM2 = new HashMap[String, Array[Double]]

  private val earlyScheduleDecision = new HashMap[String, String]

  def getEarlyScheduleDecision(stageId: String, taskId: Int): String = {
    earlyScheduleDecision.getOrElse(s"${stageId}_${taskId}", null)
  }

  def makeScheduleDecision(mapStageId: String, reduceStageId: String,
                           numMappers: Int, numReducers: Int, shuffleId: Int): Unit = {
    val scheduleUniqueId = s"${reduceStageId}_${0}"
    var scheduleDecision = Array("1", "0", "1")
    earlyScheduleDecision.update(scheduleUniqueId, "1")

    logInfo(s"Make new schedule decision: $scheduleDecision, broadcast to every Site Master")
    val result = updatePartitionSiteMethod.invoke(shuffleManager, appUniqueId,
                                                   shuffleId.asInstanceOf[Object], scheduleDecision)
  }

  // mock rpc for model m1
  private def m1(): Double = {
    10
  }

  // mock rpc for model m1
  private def m2(): Double = {
    0
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
        val oriBandwidth = bandwidthMatrix(srcSite)(dstSite)
        val newBandwidth = bandwidth
        bandwidthMatrix(srcSite)(dstSite) = newBandwidth
        if (Math.abs(newBandwidth - oriBandwidth) / oriBandwidth > rescheduleTriggerRatio) {
          // TODO: Dynamic bandwidth handler, such a mess now!
        }

    }
  }
}

private[spark] object EarlyScheduleTracker {
  val M2_PREFIX = "m2_"
  val ENDPOINT_NAME = "EarlyScheduleTracker"
}
