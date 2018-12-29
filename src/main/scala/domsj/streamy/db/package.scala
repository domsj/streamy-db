/*
 * Copyright 2018 @domsj
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package domsj.streamy

import java.util.Properties

import upickle.default._

package object db {

  type Key = String
  type Value = String
  type ValueOption = Option[Value]

  case class KeyValueOption(key: Key, valueOption: ValueOption)
  object KeyValueOption{
    implicit val rw: ReadWriter[KeyValueOption] = macroRW
  }

  type TransactionId = String

  sealed trait TransactionProcessorMessage {
    def transactionId: TransactionId
  }

  case class Transaction(transactionId: TransactionId, asserts: List[KeyValueOption], updates: List[KeyValueOption]) extends TransactionProcessorMessage
  object Transaction{
    implicit val rw = macroRW[Transaction]
  }

  case class ReadResult(transactionId: TransactionId, key: Key, valueOption: ValueOption) extends TransactionProcessorMessage

  case class TransactionResult(transaction: Transaction, succeeded: Boolean)
  object TransactionResult {
    implicit val rw = macroRW[TransactionResult]
  }

  sealed trait KeyProcessorMessage extends Product with Serializable {
    def key: Key
  }
  case class ReadRequest(transactionId: TransactionId, key: Key) extends KeyProcessorMessage
  case class LockRequest(transactionId: TransactionId, key: Key) extends KeyProcessorMessage
  case class KeyTransactionResult(transactionId: TransactionId, key: Key, valueOptionOption: Option[ValueOption]) extends KeyProcessorMessage


  val transactionInputsTopic = "transaction-inputs"
  val transactionResultsTopic = "transaction-results"

  def makeKafkaProperties() : Properties = {
    val kafkaProperties= new Properties()
    kafkaProperties.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProperties
  }

}
