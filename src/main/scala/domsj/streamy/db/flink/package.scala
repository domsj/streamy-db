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

import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

package object flink {
  def streamFromKafka(environment: StreamExecutionEnvironment,
                      topic : String)
  : DataStream[String] = {

    val logger = Logger("streamFromKafka")

    val kafkaProperties = makeKafkaProperties()

    val kafkaConsumer = new FlinkKafkaConsumer011[String](
      topic,
      new SimpleStringSchema(),
      kafkaProperties)

    val assigner: AssignerWithPunctuatedWatermarks[String] = new AssignerWithPunctuatedWatermarks[String] {
      private var previous = Long.MinValue

      override def checkAndGetNextWatermark(lastElement: String, extractedTimestamp: Long): Watermark = {
        new Watermark(extractedTimestamp)
      }

      override def extractTimestamp(element: String, elementTimestamp: Long): Long = {
        // ensure timestamps are strictly monotonic (so the watermark emission above is justified)
        val res = Math.max(elementTimestamp, previous + 1)
        previous = res
        res
      }

    }
    kafkaConsumer.assignTimestampsAndWatermarks(assigner)

    environment.addSource(kafkaConsumer)
  }

}
