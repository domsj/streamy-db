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

package domsj.streamy.db.beam

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.coders._
import com.spotify.scio.values.WindowOptions
import domsj.streamy.db._
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.state._
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{OnTimer, ProcessElement, StateId, TimerId}
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.apache.kafka.common.serialization.StringSerializer
import org.joda.time.Instant

object StreamyDb {

  class KeyedEventTimeSorter[K : Coder, V : Coder] extends DoFn[KV[K, V], KV[K, List[V]]] {
    @StateId("elements") private val elementsSpec = StateSpecs.map[Instant, KV[K, List[V]]](
          CoderMaterializer.beamWithDefault(Coder[Instant]),
          CoderMaterializer.beamWithDefault(Coder[KV[K, List[V]]])
    )
    @TimerId("timer") private val timerSpec =  TimerSpecs.timer(TimeDomain.EVENT_TIME)

    @ProcessElement
    def processElement(context: ProcessContext,
                       @StateId("elements") elements : MapState[Instant, KV[K, List[V]]],
                       @TimerId("timer") timer: Timer
                      ): Unit = {
      val timestamp = context.timestamp()
      val inputElement = context.element()
      val outputElement = Option(elements.get(timestamp).read()) match {
        case Some(v) => KV.of(inputElement.getKey, v.getValue.+:(inputElement.getValue))
        case None => KV.of(inputElement.getKey, List(inputElement.getValue))
      }
      elements.put(timestamp, outputElement)
      timer.set(timestamp)
    }

    @OnTimer("timer")
    def onTimer(context: OnTimerContext,
                @StateId("elements") elements : MapState[Instant, KV[K, List[V]]]
               ): Unit = {
      // NOTE should be able to get key : K from OnTimerContext, but beam isn't awesome enough
      val timestamp = context.timestamp()
      val element = elements.get(timestamp).read()
      context.outputWithTimestamp(element, timestamp)
      elements.remove(timestamp)
    }
  }

  class KeyTransactionProcessor extends DoFn[KV[Key, KeyProcessorMessage], KV[Key, ReadResult]] {
    @StateId("value") private val valueSpec = StateSpecs.value[Value]()
    @StateId("write-lock-waiters") private val writeLockWaitersSpec = StateSpecs.map[TransactionId, List[TransactionId]]()
    @StateId("latest-write-lock") private val latestWriteLockSpec = StateSpecs.value[TransactionId]()


    @ProcessElement
    def processElement(context: ProcessContext,
                       @StateId("value") valueState: ValueState[Value],
                       @StateId("write-lock-waiters") writeLockWaiters: MapState[TransactionId, List[TransactionId]],
                       @StateId("latest-write-lock") latestWriteLock: ValueState[TransactionId]
                      ): Unit = {

      context.element().getValue match {

        case LockRequest(transactionId, key) =>
          latestWriteLock.write(transactionId)
          writeLockWaiters.put(transactionId, Nil)

        case ReadRequest(transactionId, key) =>
          val latestWriteLockTransactionId = latestWriteLock.read()
          if (latestWriteLockTransactionId == null) {
            // no write locks, so can read value immediately
            val readResult = ReadResult(transactionId, context.element().getKey, Option(valueState.read()))
            context.outputWithTimestamp(KV.of(key, readResult), context.timestamp())
          } else {
            // one or more write locks in the queue, register with the last one
            val waiters = writeLockWaiters.get(latestWriteLockTransactionId).read()
            writeLockWaiters.put(latestWriteLockTransactionId, waiters.::(transactionId))
          }

        case KeyTransactionResult(transactionId, key, valueOptionOption) =>

          val waiters = writeLockWaiters.get(transactionId).read()
          writeLockWaiters.remove(transactionId)

          if (waiters != null) {

            // maybe update state
            valueOptionOption match {
              case Some(valueOption) =>
                valueOption match {
                  case Some(value) => valueState.write(value)
                  case None => valueState.clear()
                }
              case None => ()
            }

            val maybeValue = Option(valueState.read())
            waiters.foreach(transactionId => context.output(KV.of(key, ReadResult(transactionId, key, maybeValue))))
          }

          if (latestWriteLock.read() == transactionId) {
            latestWriteLock.clear()
          }
      }
    }
  }

  class TransactionProcessor extends DoFn[KV[TransactionId, TransactionProcessorMessage], KV[TransactionId, TransactionResult]] {
    @StateId("transaction") private val transactionSpec = StateSpecs.value[Transaction](CoderMaterializer.beamWithDefault(Coder[Transaction]))
    @StateId("read-results") private val readResultsSpec = StateSpecs.map[Key, ValueOption]()
    @StateId("read-results-count") private val readResultsCountSpec = StateSpecs.value[Int](CoderMaterializer.beamWithDefault(Coder[Int]))

    @ProcessElement
    def processElement(context: ProcessContext,
                       @StateId("transaction") transactionState: ValueState[Transaction],
                       @StateId("read-results") readResultsState: MapState[Key, ValueOption],
                       @StateId("read-results-count") readResultsCountState: ValueState[Int]
                      ): Unit = {
      val getReadResultsCount = () => Option(readResultsCountState.read()).getOrElse(0)

      // gather everything into the state
      val message = context.element().getValue
      message match {
        case Transaction(_, _, _) =>
          transactionState.write(message.asInstanceOf[Transaction])
        case ReadResult(_, key, valueOption) =>
          readResultsState.put(key, valueOption)
          readResultsCountState.write(getReadResultsCount() + 1)
      }

      // process the transaction once the state is complete
      // NOTE there is an optimisation opportunity for for failing transactions by processing ReadResults more incremental
      val transaction = transactionState.read()
      if (transaction != null && transaction.asserts.size == getReadResultsCount()) {
        val assertsAllSucceed =
          transaction
            .asserts
            .forall((keyValueOption: KeyValueOption) =>
              readResultsState.get(keyValueOption.key).read() == keyValueOption.valueOption)

        context.output(KV.of(transaction.transactionId, TransactionResult(transaction, assertsAllSucceed)))
      }
    }
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)


    val transactionsInput =
      sc.customInput("transactions-input", makeKafkaIOWithLogAppendTime(transactionInputsTopic))
        .map(v => upickle.default.read[List[Transaction]](v.getValue))
        .withGlobalWindow(
          WindowOptions(
            timestampCombiner = TimestampCombiner.LATEST,
            accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
            // trigger = Trigger.AfterAny.newBuilder().build()
          )
        )
      .flatMap(identity)

    val keyTransactionResults =
      sc.customInput("transaction-results-input", makeKafkaIOWithLogAppendTime(transactionResultsTopic))
        .map(v => upickle.default.read[TransactionResult](v.getValue))
        .flatMap(tr =>
          tr.transaction.updates.map(kvo =>
            KeyTransactionResult(
              tr.transaction.transactionId,
              kvo.key,
              if (tr.succeeded) { Some(kvo.valueOption) }  else { None })
              .asInstanceOf[KeyProcessorMessage]
          )
        )
        .keyBy(tr => tr.key)

    val sortedLockAndReadRequests = transactionsInput
      .flatMap(t => {
        val readRequests = t.asserts.map(a => ReadRequest(t.transactionId, a.key))
        val lockRequests = t.updates.map(u => LockRequest(t.transactionId, u.key))
        readRequests.++(lockRequests)
      })
      .keyBy(kpm => kpm.key)
      .applyPerKeyDoFn(new KeyedEventTimeSorter[Key, KeyProcessorMessage])
      .flatMap(messages =>
        messages
          ._2
          // for messages with the same event time, first order by
          // transactionId, and then give prio to ReadRequest over LockRequest
          .sortBy((message: KeyProcessorMessage) => message match {
          case ReadRequest(transactionId, key) => (transactionId, 0, key)
          case LockRequest(transactionId, key) => (transactionId, 1, key)
          case KeyTransactionResult(transactionId, key, _) => (transactionId, 2, key)
        })
          .map(m => (messages._1, m))
      )

    val keyProcessorMessages = sortedLockAndReadRequests.union(keyTransactionResults)

    val readResults = keyProcessorMessages.applyPerKeyDoFn(new KeyTransactionProcessor())

    val transactionResults = readResults.map(_._2.asInstanceOf[TransactionProcessorMessage])
      .union(transactionsInput.map(_.asInstanceOf[TransactionProcessorMessage]))
      .keyBy(_.transactionId)
      .applyPerKeyDoFn(new TransactionProcessor())

    val kafkaOut =
      KafkaIO
        .write[TransactionId, String]()
        .withBootstrapServers("localhost:9092")
        .withTopic(transactionResultsTopic)
        .withKeySerializer(classOf[StringSerializer])
        .withValueSerializer(classOf[StringSerializer])

    transactionResults
      .map(tr => KV.of(tr._1, upickle.default.write(tr._2)))
      .saveAsCustomOutput("transaction-results-output", kafkaOut)

    sc.pipeline.run().waitUntilFinish()
  }

}