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

package domsj.streamy.db

import java.util.UUID

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object TransactionGenerator {
  val logger = Logger(TransactionGenerator.getClass)

  def main(cmdLineArgs: Array[String]): Unit = {

    val kafkaProperties = makeKafkaProperties()
    kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](kafkaProperties)

    val push_transaction = (transaction : Transaction) => {
      logger.debug("pushing transaction {}", transaction)
      producer.send(new ProducerRecord[String, String](transactionInputsTopic, upickle.default.write(List(transaction)))).get()
    }

    val numKeys = 1000000
    val generateKey = () => "key_%010d".format(Random.nextInt(numKeys))

    val keysPerTransaction = 4
    val numTransactions = 10

    for (_ <- Range(0, numTransactions)) {
      val transactionId = UUID.randomUUID().toString
      val keys = Seq.fill(keysPerTransaction)(generateKey())

      val asserts = keys.map(k => KeyValueOption(k, None))
      val updates = keys.map(k => KeyValueOption(k, Some(k + "_value")))

      val transaction = Transaction(transactionId, asserts.toList, updates.toList)

      push_transaction(transaction)
    }
  }
}