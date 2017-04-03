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
import java.util.Random

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.netty.RequestMessage

private[netty] class Span[T](payload: T) extends Serializable with Logging {
  private def logStackTrace() = Thread.currentThread.getStackTrace.foreach(call =>
    logInfo("[STACK] " + call.toString))
  // private def getDeployFunction() = {
  //   for (StackTraceElement call : Thread.currentThread().getStackTrace())
  //     if (call.getClassName().contains("deploy")) return call.getMethodName();
  //   return "<unknown>";
  // }
  private val id = Span.nextid
  Span.nextid += 1
  private val containedClass = payload.getClass // Most of these are RequestMessages.  Can we also
  // pass the type of the message's content so that we can reconstruct that on the other side?
  private var recipient: SocketAddress = null
  private val sent = System.currentTimeMillis
  def recpt(addr: SocketAddress): Unit = {
    this.recipient = addr
    logInfo(">>>> Received: " + toString)
  }
  def details: String = payload match {
    case RequestMessage(sender, receiver, content) => "RequestMessage(" + sender + " -> " +
      (if (receiver.address != null) receiver.address else receiver.name) + ": " +
      content.getClass.getName + ")"
    case _ => containedClass.toString
  }
  override def toString: String = "Span " + id + " to " + (if (recipient != null) recipient.toString
    else "<unsent>") + " at " + sent + ": " + details
  def getPayload: T = payload
  def getId: Long = id
  def getContainedClass: Class[_] = containedClass
  logInfo(">>>> Created: " + toString)
}

object Span {
  private var nextid = new Random().nextInt.toLong + Integer.MAX_VALUE

}
