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

import java.util.concurrent.TimeUnit

import akka.actor.typed.{ActorSystem, Behavior, PostStop, Signal}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.PartitionInfo

import scala.concurrent.duration.FiniteDuration


object HeartBeatGenerator extends App {
  val logger = Logger(HeartBeatGenerator.getClass)

  val kafkaProperties = makeKafkaProperties()
  kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](kafkaProperties)

  val duration = FiniteDuration(5000, TimeUnit.MILLISECONDS)

  object PartitionHeartBeatActor {
    sealed trait Msg
    case object Timeout extends Msg
    case object KafkaPushed extends Msg

    def apply(partitionInfo: PartitionInfo): Behavior[Msg] =
      Behaviors.withTimers(timerScheduler => {
        timerScheduler.startSingleTimer(Timeout, Timeout, duration)

        Behaviors.receive[Msg]((ctx, msg) => {
          msg match {
            case Timeout =>
              logger.info("pushing heartbeat message for partition {}", partitionInfo)
              val record = new ProducerRecord[String, String](
                transactionInputsTopic,
                partitionInfo.partition(),
                "heartbeat",
                upickle.default.write[List[Transaction]](Nil)
              )
              val self = ctx.self
              producer.send(record, (_: RecordMetadata, _: Exception) => {
                logger.debug("kafka onCompletion")
                self.tell(KafkaPushed)
              })
              Behaviors.same
            case KafkaPushed =>
              logger.debug("pushed message to kafka, starting another timer")
              timerScheduler.startSingleTimer(Timeout, Timeout, duration)
              Behaviors.same
          }
        })
      })
  }

  val partitionInfos = producer.partitionsFor(transactionInputsTopic)

  object HeartBeatActor {
    def apply(): Behavior[Nothing] =
      Behaviors.setup[Nothing](context ⇒ new HeartBeatActor(context))
  }

  class HeartBeatActor(context: ActorContext[Nothing]) extends AbstractBehavior[Nothing] {
    context.log.info("HeartBeatActor started")

    partitionInfos.forEach(partitionInfo =>
      context.spawn(
        PartitionHeartBeatActor(partitionInfo),
        "%s_%s".format(partitionInfo.topic(), partitionInfo.partition())
      )
    )

    override def onMessage(msg: Nothing): Behavior[Nothing] = {
      // No need to handle any messages
      Behaviors.unhandled
    }

    override def onSignal: PartialFunction[Signal, Behavior[Nothing]] = {
      case PostStop ⇒
        context.log.info("HeartBeat Application stopped")
        this
    }
  }

  ActorSystem[Nothing](HeartBeatActor(), "heart-beats")
}