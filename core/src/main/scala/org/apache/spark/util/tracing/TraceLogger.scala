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

package org.apache.spark.util.tracing

import java.lang.management.ManagementFactory
import java.net.{InetSocketAddress, SocketAddress}
import java.util.UUID

import io.netty.channel.Channel

import org.apache.spark.SparkFirehoseListener
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.storage.{BlockId, BlockManagerId}

sealed trait TraceEvent extends Product with Serializable
final case class DebugMessage(msg: String) extends TraceEvent
case object JVMStart extends TraceEvent
case object MainStart extends TraceEvent
case object MainEnd extends TraceEvent
final case class SpawnExecutor(id: String) extends TraceEvent
final case class TrackerRegisterShuffle(id: Int) extends TraceEvent
final case class RegisterShuffle(id: Int) extends TraceEvent
final case class UnregisterShuffle(id: Int) extends TraceEvent
final case class BlockFetch(host: String, port: Int, execId: String, blockIds: Seq[String]) extends
  TraceEvent
final case class BlockUpload(host: String, port: Int, execId: String, blockId: String) extends
  TraceEvent
final case class SubmitTaskSet(id: String) extends TraceEvent
final case class FinishTaskSet(id: String) extends TraceEvent
final case class DagSchedulerEvent(event: Any) extends TraceEvent
final case class ListenerEvent(event: SparkListenerEvent) extends TraceEvent
final case class GetBlock(id: BlockId) extends TraceEvent
final case class GetBlockData(id: BlockId) extends TraceEvent
final case class PutBlock(id: BlockId) extends TraceEvent
final case class DeleteBlock(id: BlockId) extends TraceEvent
final case class FreeBlock(id: BlockId) extends TraceEvent
final case class BMMRegister(id: BlockManagerId) extends TraceEvent
final case class BMMUpdate(id: BlockId) extends TraceEvent
final case class BMMRemoveBlock(id: BlockId) extends TraceEvent
final case class BMMRemoveRDD(id: Int) extends TraceEvent
final case class BMMRemoveShuffle(id: Int) extends TraceEvent
final case class BMMRemoveBroadcast(id: Long) extends TraceEvent
final case class Distribute(path: String) extends TraceEvent
final case class DistributeDone(path: String) extends TraceEvent
case object SparkUpload extends TraceEvent
case object SparkUploadDone extends TraceEvent
case object SubmitApplication extends TraceEvent
case object StartYarnClient extends TraceEvent
final case class SubmittedApplication(id: Int) extends TraceEvent
case object CreateSparkContext extends TraceEvent
case object InitializedSparkContext extends TraceEvent
final case class SparkHost(name: String, host: String, port: Int)
case class ChannelInfo(src: SparkHost, dst: SparkHost)
final case class RPC(src: SparkHost, dst: SparkHost, payload: Any) extends TraceEvent
case object FetchDriverProps extends TraceEvent
case object CreateSparkEnv extends TraceEvent
case object SetupEndpoint extends TraceEvent
case object ExecutorDone extends TraceEvent
object RPC
{
  def channelInfo(channel: Channel, sendName: String, recvName: String): ChannelInfo = {
    def addrsplit(addr: SocketAddress) = if (addr == null) ("", 0) else {
      val inetAddr = addr.asInstanceOf[InetSocketAddress]
      (inetAddr.getAddress.toString, inetAddr.getPort)
    }
    val sender = addrsplit(channel.remoteAddress)
    val recipient = addrsplit(channel.localAddress)
    ChannelInfo(SparkHost(sendName, sender._1, sender._2), SparkHost(recvName, recipient._1,
      recipient._2))
  }
  def apply(channel: Channel, recvName: String, payload: Any): RPC = {
    val info = channelInfo(channel, "?", recvName)
    RPC(info.src, info.dst, payload)
  }
}

object TraceLogger extends Logging {
  val id: String = UUID.randomUUID.toString
  val cols = List("id", "time", "type")
  private def write(values: Map[String, String]): Unit = logTrace(cols.map(values(_)).
    mkString("\t"))
  def log(event: => TraceEvent, time: Long = System.currentTimeMillis): Unit = {
    if (isTraceEnabled()) write(Map("id" -> id, "time" -> time.toString, "type" ->
      event.toString.split("\n")(0)))
  }
  def logStartup(): Unit = {
    val now = System.currentTimeMillis
    val start = ManagementFactory.getRuntimeMXBean.getStartTime
    log(JVMStart, start)
    log(MainStart, now)
  }
  logTrace(cols.mkString("\t"))
}

class ListenerTraceLogger() extends SparkFirehoseListener() {
  override def onEvent(event: SparkListenerEvent): Unit = TraceLogger.log(ListenerEvent(event))
}
