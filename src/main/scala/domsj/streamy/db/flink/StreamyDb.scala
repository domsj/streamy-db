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

package domsj.streamy.db.flink

import com.typesafe.scalalogging.Logger
import domsj.streamy.db._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.util.Collector

object StreamyDb {

  class KeyedEventTimeSorter[K, V] extends KeyedProcessFunction[K, V, List[V]] {
    private val logger = Logger[KeyedEventTimeSorter[K, V]]

    private var elements : MapState[Long, List[V]] = _

    override def open(parameters: Configuration): Unit = {
      elements = getRuntimeContext.getMapState(new MapStateDescriptor("elements", classOf[Long], classOf[List[V]]))
    }

    override def processElement(value: V,
                                ctx: KeyedProcessFunction[K, V, List[V]]#Context,
                                out: Collector[List[V]]
                               ): Unit = {
      logger.info("processElements: {}", value)

      val timestamp = ctx.timestamp()
      val timestampElements = Option(elements.get(timestamp)).getOrElse(Nil)
      elements.put(timestamp, timestampElements.+:(value))
      ctx.timerService().registerEventTimeTimer(timestamp)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[K, V, List[V]]#OnTimerContext,
                         out: Collector[List[V]]
                        ): Unit = {
      logger.info("onTimer: {}", timestamp)

      out.collect(elements.get(timestamp))
    }
  }

  class KeyTransactionProcessor extends KeyedProcessFunction[Key, KeyProcessorMessage, ReadResult]{
    private val logger = Logger[KeyTransactionProcessor]

    private var value : ValueState[Value] = _
    private var writeLockWaiters : MapState[TransactionId, List[TransactionId]] = _
    private var latestWriteLock : ValueState[TransactionId] = _


    override def open(parameters: Configuration): Unit = {
      value = getRuntimeContext.getState(new ValueStateDescriptor("value", classOf[Value]))
      writeLockWaiters = getRuntimeContext.getMapState(new MapStateDescriptor("writeLockWaiters", classOf[TransactionId], classOf[List[TransactionId]]))
      latestWriteLock = getRuntimeContext.getState(new ValueStateDescriptor("latestWriteLock", classOf[TransactionId]))
    }

    override def processElement(kpm: KeyProcessorMessage,
                                ctx: KeyedProcessFunction[Key, KeyProcessorMessage, ReadResult]#Context,
                                out: Collector[ReadResult]): Unit = {
      logger.info("processElement: {}", kpm)

      kpm match {

        case LockRequest(transactionId, key) =>
          latestWriteLock.update(transactionId)
          writeLockWaiters.put(transactionId, Nil)

        case ReadRequest(transactionId, key) =>
          val latestWriteLockTransactionId = latestWriteLock.value()
          if (latestWriteLockTransactionId == null) {
            // no write locks, so can read value immediately
            val readResult = ReadResult(transactionId, ctx.getCurrentKey, Option(value.value()))
            out.collect(readResult)
          } else {
            // one or more write locks in the queue, register with the last one
            val waiters = writeLockWaiters.get(latestWriteLockTransactionId)
            writeLockWaiters.put(latestWriteLockTransactionId, waiters.::(transactionId))
          }

        case KeyTransactionResult(transactionId, key, valueOptionOption) =>

          val waiters = writeLockWaiters.get(transactionId)
          writeLockWaiters.remove(transactionId)

          if (waiters != null) {

            // maybe update state
            valueOptionOption match {
              case Some(valueOption) =>
                valueOption match {
                  case Some(v) => value.update(v)
                  case None => value.clear()
                }
              case None => ()
            }

            val maybeValue = Option(value.value())
            waiters.foreach(transactionId => out.collect(ReadResult(transactionId, key, maybeValue)))
          }

          if (latestWriteLock.value() == transactionId) {
            latestWriteLock.clear()
          }
      }
    }
  }

  class TransactionProcessor extends KeyedProcessFunction[TransactionId, TransactionProcessorMessage, TransactionResult] {
    private val logger = Logger[TransactionProcessor]

    private var transactionState : ValueState[Transaction] = _
    private var readResultsState : MapState[Key, ValueOption] = _
    private var readResultsCountState : ValueState[Int] = _


    override def open(parameters: Configuration): Unit = {
      transactionState = getRuntimeContext.getState(new ValueStateDescriptor("transaction", classOf[Transaction]))
      readResultsState = getRuntimeContext.getMapState(new MapStateDescriptor("read-results", classOf[Key], classOf[ValueOption]))
      readResultsCountState = getRuntimeContext.getState(new ValueStateDescriptor("read-results-count", classOf[Int]))
    }

    override def processElement(value: TransactionProcessorMessage,
                                ctx: KeyedProcessFunction[TransactionId, TransactionProcessorMessage, TransactionResult]#Context,
                                out: Collector[TransactionResult]
                               ): Unit = {
      logger.info("processElement: {}", value)

      val getReadResultsCount = () => Option(readResultsCountState.value()).getOrElse(0)

      // gather everything into the state
      value match {
        case Transaction(_, _, _) =>
          transactionState.update(value.asInstanceOf[Transaction])
        case ReadResult(_, key, valueOption) =>
          readResultsState.put(key, valueOption)
          readResultsCountState.update(getReadResultsCount() + 1)
      }

      // process the transaction once the state is complete
      // NOTE there is an optimisation opportunity for for failing transactions by processing ReadResults more incremental
      val transaction = transactionState.value()
      if (transaction != null && transaction.asserts.size == getReadResultsCount()) {
        val assertsAllSucceed =
          transaction
            .asserts
            .forall((keyValueOption: KeyValueOption) =>
              readResultsState.get(keyValueOption.key) == keyValueOption.valueOption)

        out.collect(TransactionResult(transaction, assertsAllSucceed))
      }
    }
  }

  def main(cmdLineArgs: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.enableCheckpointing(5000)
    environment.setStateBackend(new MemoryStateBackend().asInstanceOf[StateBackend])

    val kafkaProperties = makeKafkaProperties()

    val transactionInputs =
      streamFromKafka(environment, transactionInputsTopic)
        .flatMap(s => upickle.default.read[List[Transaction]](s))

    val keyTransactionResults =
      streamFromKafka(environment, transactionResultsTopic)
        .map(s => upickle.default.read[TransactionResult](s))
        .flatMap(tr =>
          tr.transaction.updates.map(kvo =>
            KeyTransactionResult(
              tr.transaction.transactionId,
              kvo.key,
              if (tr.succeeded) { Some(kvo.valueOption) }  else { None })
              .asInstanceOf[KeyProcessorMessage]
          )
        )


    val sortedLockAndReadRequests = transactionInputs
      .flatMap(t => {
        val readRequests = t.asserts.map(a => ReadRequest(t.transactionId, a.key))
        val lockRequests = t.updates.map(u => LockRequest(t.transactionId, u.key))
        readRequests.++(lockRequests)
      })
      .keyBy(kpm => kpm.key)
      .process(new KeyedEventTimeSorter[TransactionId, KeyProcessorMessage]())
      .flatMap(l => l.sortBy((message: KeyProcessorMessage) => message match {
        case ReadRequest(transactionId, key) => (transactionId, 0, key)
        case LockRequest(transactionId, key) => (transactionId, 1, key)
        case KeyTransactionResult(transactionId, key, _) => (transactionId, 2, key)
      }))

    sortedLockAndReadRequests.print("sortedLockAndReadRequests")

    val keyProcessorMessages = sortedLockAndReadRequests.union(keyTransactionResults)

    val readResults = keyProcessorMessages
      .keyBy(_.key)
      .process(new KeyTransactionProcessor())

    readResults.print("readResults")

    val transactionResults =
      readResults.map(_.asInstanceOf[TransactionProcessorMessage])
        .union(transactionInputs.map(_.asInstanceOf[TransactionProcessorMessage]))
        .keyBy(_.transactionId)
        .process(new TransactionProcessor())

    transactionResults
      .map(tr => upickle.default.write(tr))
      .addSink(new FlinkKafkaProducer011[String](transactionResultsTopic, new SimpleStringSchema(), kafkaProperties))

    transactionResults.print("transactionResults")

    val jobExecutionResult = environment.execute()

    print(jobExecutionResult)
  }
}
