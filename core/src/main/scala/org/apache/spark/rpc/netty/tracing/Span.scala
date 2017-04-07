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

package org.apache.spark.rpc.netty.tracing

import java.io.{FileWriter, Serializable, Writer}
import java.net.SocketAddress
import java.util.Random

import io.netty.channel.Channel

import org.apache.spark.util.tracing._

private[netty] class Span[T](payload: T, writer: SpanWriter) extends Serializable {
  // private def logStackTrace() = Thread.currentThread.getStackTrace.foreach(call =>
  //   logInfo("[STACK] " + call.toString))
  private val id = writer.makeId
  private var origin, recipient = Array("", "")
  private val srcName = writer.getName
  private val sent = System.currentTimeMillis

  private def write(out: SpanWriter): Unit = {
    out.write(Map("id" -> id.toString, "time" -> sent.toString, "src_name" -> srcName,
      "src_addr" -> origin(0), "src_port" -> origin(1), "dst_name" -> out.getName,
      "dst_addr" -> recipient(0), "dst_port" -> recipient(1), "type" -> payload.toString))
  }
  def recpt(channel: Channel, writer: SpanWriter): Unit = {
    def addrsplit(addr: SocketAddress) = if (addr != null) addr.toString.substring(1).split(":")
      else Array("", "")
    origin = addrsplit(channel.remoteAddress)
    recipient = addrsplit(channel.localAddress)
    write(writer) // logInfo(">>>> Received: " + toString)
  }
  def getPayload: T = payload
  // logInfo(">>>> Created: " + toString)
}

private[netty] class SpanWriter(name: String) {
  private var nextid = new Random().nextInt.toLong + Integer.MAX_VALUE
  def getName: String = name
  def makeId: Long = {
    nextid += 1
    nextid
  }
  private[tracing] def write(values: Map[String, String]): Unit = SpanWriter.write(values)
}

private[netty] object SpanWriter {
  private val output: Writer = new FileWriter("/tmp/spark-rpc_" + EventTrace.getId + ".tsv")
  private val columns = List("id", "time", "src_name", "src_addr", "src_port", "dst_name",
    "dst_addr", "dst_port", "type")
  private[tracing] def write(values: Map[String, String]): Unit = this.synchronized {
    output.write(columns.map(values(_)).mkString("\t") + "\n")
    output.flush()
  }
  output.write(columns.mkString("\t") + "\n")
}
