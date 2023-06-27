/*
 * Copyright 2023 Medicines Discovery Catapult
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mdcatapult.doclib.handlers

import akka.Done
import akka.stream.alpakka.amqp.ReadResult
import akka.stream.alpakka.amqp.scaladsl.CommittableReadResult
import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import io.mdcatapult.doclib.messages.PrefetchMsg

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Convenience class to create a CommittableReadResult for testing
 *
 * @param msg
 */
case class PrefetchMsgCommittableReadResult(msg: PrefetchMsg) extends CommittableReadResult {
  override val message: ReadResult = ReadResult(ByteString.apply(msg.toJsonString()), new com.rabbitmq.client.Envelope(1234, true, "", ""), new BasicProperties.Builder().build())

  override def ack(multiple: Boolean): Future[Done] = {
    Future(Done)
  }

  override def nack(multiple: Boolean, requeue: Boolean): Future[Done] = {
    Future(Done)
  }
}
