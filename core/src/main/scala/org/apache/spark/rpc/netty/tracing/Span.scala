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

import java.io.Serializable
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicLong
import java.util.Random

import io.netty.channel.Channel

import org.apache.spark.util.tracing._

private[netty] class Span[T](payload: T, writer: SpanWriter) extends Serializable {
  private case class ReceiveInfo(source: (String, String), destination: (String, String))
  private val id = writer.makeId
  private val srcName = writer.name
  private val sent = System.currentTimeMillis

  private def write(out: SpanWriter, info: ReceiveInfo): Unit = {
    RpcTraceLogger.write(Map("id" -> id.toString, "time" -> sent.toString, "src_name" -> srcName,
      "src_addr" -> info.source._1, "src_port" -> info.source._2, "dst_name" -> out.name,
      "dst_addr" -> info.destination._1, "dst_port" -> info.destination._2, "type" ->
      payload.toString))
  }
  def recpt(channel: Channel, writer: SpanWriter): Unit = {
    def addrsplit(addr: SocketAddress) = if (addr == null) ("", "") else addr.toString
      .substring(1).split(":") match {
      case Array(x, y) => (x, y)
    }
    val info = ReceiveInfo(addrsplit(channel.remoteAddress()), addrsplit(channel.localAddress()))
    write(writer, info)
  }
  def getPayload: T = payload
}

private[netty] class SpanWriter(val name: String) {
  private var nextid = new AtomicLong(new Random().nextInt.toLong + Integer.MAX_VALUE)
  def makeId: Long = nextid.incrementAndGet
}
