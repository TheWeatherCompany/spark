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
import java.util.concurrent.atomic.AtomicLong
import java.util.Random

import io.netty.channel.Channel

import org.apache.spark.util.tracing._

private[netty] class Span[T](payload: T, writer: SpanInfo) extends Serializable {
  private val id = writer.makeId
  private val srcName = writer.name
  private val sent = System.currentTimeMillis

  private def write(info: ChannelInfo): Unit = {
    TraceLogger.log(RPC(info.src, info.dst, payload), sent)
  }
  def recpt(channel: Channel, spanInfo: SpanInfo): Unit = {
    val recvInfo = TraceLogger.channelInfo(channel, srcName, spanInfo.name)
    write(recvInfo)
  }
  def getPayload: T = payload
}

private[netty] class SpanInfo(val name: String) {
  private var nextid = new AtomicLong(new Random().nextInt.toLong + Integer.MAX_VALUE)
  def makeId: Long = nextid.incrementAndGet
}
