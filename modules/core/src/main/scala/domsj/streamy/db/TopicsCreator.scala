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

import java.util.Collections

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.config.TopicConfig

import scala.collection.JavaConverters._

object TopicsCreator extends App {
  val logger = Logger(TopicsCreator.getClass)

  // TODO change replication factor!
  val replicationFactor : Short = 1
  val inputsTopic = new NewTopic(transactionInputsTopic, 4, replicationFactor)

  // https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message
  // The good things about LogAppendTime are: 1) ... 2) Monotonically increasing.
  inputsTopic.configs(Map(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG -> "LogAppendTime").asJava)

  val adminClient = AdminClient.create(makeKafkaProperties())
  val result = adminClient.createTopics(Collections.singleton(inputsTopic))

  result.all().get()
}