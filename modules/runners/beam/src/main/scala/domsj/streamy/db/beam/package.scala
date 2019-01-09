package domsj.streamy.db

import java.util.Optional

import org.apache.beam.sdk.io.kafka._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.joda.time.Instant

package object beam {

  def makeKafkaIOWithLogAppendTime(topic : String) = {
    KafkaIO
      .read()
      .withKeyDeserializer(classOf[StringDeserializer])
      .withValueDeserializer(classOf[StringDeserializer])
      .withBootstrapServers("localhost:9092")
      .withTopic(topic)
      .withTimestampPolicyFactory(new TimestampPolicyFactory[String, String] {
      override def createTimestampPolicy(tp: TopicPartition,
                                         previousWatermark: Optional[Instant]
                                        ): TimestampPolicy[String, String] = {

        new TimestampPolicy[String, String] {

          private var currentWaterMark = previousWatermark.orElse(new Instant(0))

          override def getTimestampForRecord(ctx: TimestampPolicy.PartitionContext,
                                             record: KafkaRecord[String, String]): Instant = {

            if (record.getTimestampType != KafkaTimestampType.LOG_APPEND_TIME) {
              throw new IllegalStateException("Wrong kafka record timestamp type. Expected LOG_APPEND_TIME but got %s instead".format(record.getTimestampType))
            }

            currentWaterMark = new Instant(record.getTimestamp)
            currentWaterMark
          }

          override def getWatermark(ctx: TimestampPolicy.PartitionContext): Instant = {
            currentWaterMark
          }
        }
      }
    })
      .withReadCommitted()
      .withoutMetadata()

  }
}