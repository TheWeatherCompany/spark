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
import java.util.UUID

import org.slf4j.LoggerFactory

trait TraceEvent
case object JVMStart extends TraceEvent
case object MainStart extends TraceEvent
case object MainEnd extends TraceEvent
case class SpawnExecutor(id: String) extends TraceEvent
case class TrackerRegisterShuffle(id: Int) extends TraceEvent
case class RegisterShuffle(id: Int) extends TraceEvent
case class UnregisterShuffle(id: Int) extends TraceEvent
case class BlockFetch(host: String, port: Int, execId: String, blockIds: Seq[String]) extends
  TraceEvent
case class BlockUpload(host: String, port: Int, execId: String, blockId: String) extends TraceEvent
case class SubmitTaskSet(id: String) extends TraceEvent
case class DagScheduler(event: Any) extends TraceEvent

abstract class TraceLogger(cls: String) {
  private val logger = LoggerFactory.getLogger(cls)
  def write(values: Map[String, String]): Unit = logger.trace(columns.map(values(_)).mkString("\t"))
  val columns: List[String]
  protected def init = logger.trace(columns.mkString("\t"))
}

object TraceId {
  val id: String = UUID.randomUUID.toString
}

object EventTraceLogger extends TraceLogger("org.apache.spark.util.tracing.EventTraceLogger") {
  override val columns = List("id", "time", "type")
  def log(event: TraceEvent, time: Long = System.currentTimeMillis): Unit = {
    write(Map("id" -> TraceId.id, "time" -> time.toString, "type" -> event.toString.split("\n")(0)))
  }
  def logStartup(): Unit = {
    val now = System.currentTimeMillis
    val start = ManagementFactory.getRuntimeMXBean.getStartTime
    log(JVMStart, start)
    log(MainStart, now)
  }
  init
}

object RpcTraceLogger extends TraceLogger("org.apache.spark.util.tracing.RpcTraceLogger") {
  override val columns = List("id", "time", "src_name", "src_addr", "src_port", "dst_name",
    "dst_addr", "dst_port", "type")
  init
}
