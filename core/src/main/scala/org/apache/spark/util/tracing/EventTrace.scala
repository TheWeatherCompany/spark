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

import java.io.{FileWriter, Writer}
import java.lang.management.ManagementFactory
import java.util.UUID

trait TraceEvent
case class JVMStart() extends TraceEvent
case class MainStart() extends TraceEvent
case class MainEnd() extends TraceEvent
case class SpawnExecutor(id: String) extends TraceEvent

object EventTrace {
  private val id = UUID.randomUUID.toString // System.currentTimeMillis.toString
  private val output: Writer = new FileWriter("/tmp/spark-event_" + id + ".tsv")
  private val columns = List("id", "time", "type")
  private def write(values: Map[String, String]) = this.synchronized {
    output.write(columns.map(values(_)).mkString("\t") + "\n")
    output.flush()
  }
  def getId: String = id
  def log(event: TraceEvent, time: Long = System.currentTimeMillis): Unit = {
    write(Map("id" -> id, "time" -> time.toString, "type" -> event.toString))
  }
  def logStartup(): Unit = {
    val now = System.currentTimeMillis
    val start = ManagementFactory.getRuntimeMXBean.getStartTime
    log(JVMStart(), start)
    log(MainStart(), now)
  }
  output.write(columns.mkString("\t") + "\n")
}
