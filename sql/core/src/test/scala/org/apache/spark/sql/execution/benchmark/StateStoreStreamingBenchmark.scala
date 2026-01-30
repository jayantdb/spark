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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.util.Utils
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.sql.Timestamp
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

// Data schema defined outside class for Spark encoder compatibility
case class BenchmarkEvent(
    userId: String,
    eventType: String,
    value: Long,
    timestamp: Timestamp
)

case class BenchmarkUserSession(
    userId: String,
    sessionStart: Timestamp,
    sessionEnd: Timestamp,
    eventCount: Long,
    totalValue: Long
)

case class BenchmarkUserState(
    lastEventTime: Long,
    eventCount: Long,
    totalValue: Long
)

case class BenchmarkResult(
    testName: String,
    providerName: String,
    totalTimeMs: Long,
    avgBatchTimeMs: Long,
    eventsPerSecond: Long,
    peakStateSize: Long,
    batchTimes: Seq[Long]
)

/**
 * Real Streaming Benchmark Suite for StateStore implementations.
 *
 * This benchmark tests HDFS, RocksDB, and LSMTree StateStores with actual Streaming queries
 * including:
 * - Windowed aggregations
 * - Stateful operations (flatMapGroupsWithState)
 * - Stream deduplication
 * - Session windows
 * - Multiple aggregations
 *
 * Run with: build/sbt "sql/testOnly *StateStoreStreamingBenchmark"
 */
class StateStoreStreamingBenchmark extends SparkFunSuite with BeforeAndAfterAll {
  private val NUM_EVENTS = 100000       // Events per batch
  private val NUM_BATCHES = 10          // Number of batches to process
  private val NUM_KEYS = 10000          // Unique keys (affects state size)
  private val WATERMARK_DELAY = "30 seconds"

  private val stateStoreProviders = Seq(
    ("HDFS", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"),
    ("RocksDB", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"),
    ("LSMTree", "org.apache.spark.sql.execution.streaming.state.LSMTreeStateStoreProvider")
  )

  // Results storage
  private val benchmarkResults = new ArrayBuffer[BenchmarkResult]()

  // Implicit Encoders for MemoryStream
  implicit val eventEncoder: Encoder[BenchmarkEvent] = Encoders.product[BenchmarkEvent]
  implicit val userSessionEncoder: Encoder[BenchmarkUserSession] =
    Encoders.product[BenchmarkUserSession]
  implicit val userStateEncoder: Encoder[BenchmarkUserState] = Encoders.product[BenchmarkUserState]

  test("Benchmark: Windowed Aggregation (Tumbling Window)") {
    runBenchmark("Windowed Aggregation") { (spark, checkpointDir) =>
      import spark.implicits._

      implicit val sparkSession: SparkSession = spark
      val memoryStream = MemoryStream[BenchmarkEvent]

      val windowedAgg = memoryStream.toDS()
        .withWatermark("timestamp", WATERMARK_DELAY)
        .groupBy(
          window($"timestamp", "1 minute"),
          $"userId"
        )
        .agg(
          count("*").as("eventCount"),
          sum("value").as("totalValue"),
          avg("value").as("avgValue")
        )

      runStreamingQuery(
        spark,
        memoryStream,
        windowedAgg,
        checkpointDir,
        "windowedAgg"
      )
    }
  }

  test("Benchmark: Sliding Window Aggregation") {
    runBenchmark("Sliding Window") { (spark, checkpointDir) =>
      import spark.implicits._

      implicit val sparkSession: SparkSession = spark
      val memoryStream = MemoryStream[BenchmarkEvent]

      val slidingWindowAgg = memoryStream.toDS()
        .withWatermark("timestamp", WATERMARK_DELAY)
        .groupBy(
          window($"timestamp", "2 minutes", "30 seconds"),
          $"eventType"
        )
        .agg(
          count("*").as("eventCount"),
          sum("value").as("totalValue"),
          max("value").as("maxValue"),
          min("value").as("minValue")
        )

      runStreamingQuery(
        spark,
        memoryStream,
        slidingWindowAgg,
        checkpointDir,
        "slidingWindow"
      )
    }
  }

  test("Benchmark: Stream Deduplication") {
    runBenchmark("Deduplication") { (spark, checkpointDir) =>

      implicit val sparkSession: SparkSession = spark
      val memoryStream = MemoryStream[BenchmarkEvent]

      // Generate events with duplicates
      val deduplicated = memoryStream.toDS()
        .withWatermark("timestamp", WATERMARK_DELAY)
        .dropDuplicatesWithinWatermark("userId", "eventType")

      runStreamingQuery(
        spark,
        memoryStream,
        deduplicated,
        checkpointDir,
        "deduplication"
      )
    }
  }

  test("Benchmark: Multiple Aggregations") {
    runBenchmark("Multiple Aggregations") { (spark, checkpointDir) =>
      import spark.implicits._

      implicit val sparkSession: SparkSession = spark
      val memoryStream = MemoryStream[BenchmarkEvent]

      val multiAgg = memoryStream.toDS()
        .withWatermark("timestamp", WATERMARK_DELAY)
        .groupBy(
          window($"timestamp", "1 minute"),
          $"userId",
          $"eventType"
        )
        .agg(
          count("*").as("eventCount"),
          sum("value").as("totalValue"),
          avg("value").as("avgValue"),
          stddev("value").as("stddevValue"),
          max("value").as("maxValue"),
          min("value").as("minValue"),
          first("timestamp").as("firstEvent"),
          last("timestamp").as("lastEvent")
        )

      runStreamingQuery(
        spark,
        memoryStream,
        multiAgg,
        checkpointDir,
        "multiAgg"
      )
    }
  }

  test("Benchmark: High Cardinality Keys") {
    runBenchmark("High Cardinality (100K keys)") { (spark, checkpointDir) =>
      import spark.implicits._

      implicit val sparkSession: SparkSession = spark
      val memoryStream = MemoryStream[BenchmarkEvent]

      val agg = memoryStream.toDS()
        .withWatermark("timestamp", WATERMARK_DELAY)
        .groupBy(
          window($"timestamp", "1 minute"),
          $"userId"
        )
        .agg(
          count("*").as("eventCount"),
          sum("value").as("totalValue")
        )

      // Use high cardinality keys
      runStreamingQueryWithHighCardinality(
        spark,
        memoryStream,
        agg,
        checkpointDir
      )
    }
  }

  test("Benchmark: Update Mode Aggregation") {
    runBenchmark("Update Mode") { (spark, checkpointDir) =>
      import spark.implicits._

      implicit val sparkSession: SparkSession = spark
      val memoryStream = MemoryStream[BenchmarkEvent]

      val updateAgg = memoryStream.toDS()
        .groupBy($"userId", $"eventType")
        .agg(
          count("*").as("eventCount"),
          sum("value").as("totalValue"),
          max("value").as("maxValue")
        )

      runStreamingQueryUpdateMode(
        spark,
        memoryStream,
        updateAgg,
        checkpointDir
      )
    }
  }

  test("Benchmark: FlatMapGroupsWithState (Session Tracking)") {
    runBenchmark("FlatMapGroupsWithState") { (spark, checkpointDir) =>
      import spark.implicits._

      implicit val sparkSession: SparkSession = spark
      val memoryStream = MemoryStream[BenchmarkEvent]

      val sessionTimeout = 60000L // 1 minute session timeout

      def updateSessionState(
          userId: String,
          events: Iterator[BenchmarkEvent],
          state: GroupState[BenchmarkUserState]): Iterator[BenchmarkUserSession] = {

        val eventsList = events.toList
        if (eventsList.isEmpty) {
          if (state.hasTimedOut) {
            val oldState = state.get
            state.remove()
            Iterator(BenchmarkUserSession(
              userId,
              new Timestamp(oldState.lastEventTime - (oldState.eventCount * 1000)),
              new Timestamp(oldState.lastEventTime),
              oldState.eventCount,
              oldState.totalValue
            ))
          } else {
            Iterator.empty
          }
        } else {
          val currentState = state.getOption.getOrElse(BenchmarkUserState(0L, 0L, 0L))
          val newEventCount = currentState.eventCount + eventsList.size
          val newTotalValue = currentState.totalValue + eventsList.map(_.value).sum
          val lastEventTime = eventsList.map(_.timestamp.getTime).max

          val newState = BenchmarkUserState(lastEventTime, newEventCount, newTotalValue)
          state.update(newState)
          state.setTimeoutTimestamp(lastEventTime + sessionTimeout)

          Iterator.empty
        }
      }

      val sessions = memoryStream.toDS()
        .withWatermark("timestamp", WATERMARK_DELAY)
        .groupByKey(_.userId)
        .flatMapGroupsWithState(
          OutputMode.Append(),
          GroupStateTimeout.EventTimeTimeout()
        )(updateSessionState)

      runStreamingQuery(
        spark,
        memoryStream,
        sessions,
        checkpointDir,
        "flatMapGroupsWithState"
      )
    }
  }

  private def runBenchmark(testName: String)(
      testFn: (SparkSession, String) => BenchmarkResult): Unit = {

    // scalastyle:off println
    println(s"\n${"=" * 80}")
    println(s"BENCHMARK: $testName")
    println(s"${"=" * 80}")
    println(s"Config: events=$NUM_EVENTS, batches=$NUM_BATCHES, keys=$NUM_KEYS")
    println()
    // scalastyle:on println

    for ((_, providerClass) <- stateStoreProviders) {
      val checkpointDir = Utils.createTempDir().getAbsolutePath

      try {
        val spark = createSparkSession(providerClass, checkpointDir)

        try {
          val result = testFn(spark, checkpointDir)
          benchmarkResults += result
          printResult(result)
        } finally {
          spark.stop()
        }
      } finally {
        Utils.deleteRecursively(new File(checkpointDir))
      }
    }

    printComparison(testName)
  }

  private def createSparkSession(providerClass: String, checkpointDir: String): SparkSession = {
    SparkSession.builder()
      .master("local[4]")
      .appName(s"StateStoreBenchmark-${UUID.randomUUID()}")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.streaming.stateStore.providerClass", providerClass)
      .config("spark.sql.streaming.checkpointLocation", checkpointDir)
      .config("spark.sql.streaming.stateStore.minDeltasForSnapshot", "5")
      // LSMTree specific configs
      .config("spark.sql.streaming.stateStore.lsmtree.memTableSizeBytes", "16777216") // 16MB
      .config("spark.sql.streaming.stateStore.lsmtree.blockSizeBytes", "4096")
      .config("spark.sql.streaming.stateStore.lsmtree.bloomFilterFpp", "0.01")
      // RocksDB specific configs
      .config("spark.sql.streaming.stateStore.rocksdb.compactOnCommit", "false")
      .getOrCreate()
  }

  private def runStreamingQuery[T](
      spark: SparkSession,
      memoryStream: MemoryStream[BenchmarkEvent],
      result: Dataset[T],
      checkpointDir: String,
      queryName: String): BenchmarkResult = {

    val batchTimes = new ArrayBuffer[Long]()
    val startTime = System.currentTimeMillis()
    var peakStateSize = 0L

    val query = result.writeStream
      .format("memory")
      .queryName(queryName)
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", s"$checkpointDir/$queryName")
      .trigger(Trigger.ProcessingTime(0))
      .start()

    try {
      for (batch <- 0 until NUM_BATCHES) {
        val batchStart = System.currentTimeMillis()

        // Generate events for this batch
        val events = generateEvents(batch)
        memoryStream.addData(events)

        // Wait for batch to complete
        query.processAllAvailable()

        val batchTime = System.currentTimeMillis() - batchStart
        batchTimes += batchTime

        // Get state size from progress
        val progress = query.lastProgress
        if (progress != null && progress.stateOperators.nonEmpty) {
          val stateSize = progress.stateOperators.map(_.numRowsTotal).sum
          if (stateSize > peakStateSize) {
            peakStateSize = stateSize
          }
        }
      }
    } finally {
      query.stop()
    }

    val totalTime = System.currentTimeMillis() - startTime
    val totalEvents = NUM_EVENTS.toLong * NUM_BATCHES
    val eventsPerSecond = if (totalTime > 0) (totalEvents * 1000) / totalTime else 0

    BenchmarkResult(
      testName = queryName,
      providerName = getProviderNameFromQuery(spark),
      totalTimeMs = totalTime,
      avgBatchTimeMs = batchTimes.sum / batchTimes.size,
      eventsPerSecond = eventsPerSecond,
      peakStateSize = peakStateSize,
      batchTimes = batchTimes.toSeq
    )
  }

  private def runStreamingQueryWithHighCardinality[T](
      spark: SparkSession,
      memoryStream: MemoryStream[BenchmarkEvent],
      result: Dataset[T],
      checkpointDir: String): BenchmarkResult = {

    val batchTimes = new ArrayBuffer[Long]()
    val startTime = System.currentTimeMillis()
    var peakStateSize = 0L
    val queryName = "highCardinality"

    val query = result.writeStream
      .format("memory")
      .queryName(queryName)
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", s"$checkpointDir/$queryName")
      .trigger(Trigger.ProcessingTime(0))
      .start()

    try {
      for (batch <- 0 until NUM_BATCHES) {
        val batchStart = System.currentTimeMillis()

        // Generate high cardinality events
        val events = generateHighCardinalityEvents(batch, 100000)
        memoryStream.addData(events)

        query.processAllAvailable()

        val batchTime = System.currentTimeMillis() - batchStart
        batchTimes += batchTime

        val progress = query.lastProgress
        if (progress != null && progress.stateOperators.nonEmpty) {
          val stateSize = progress.stateOperators.map(_.numRowsTotal).sum
          if (stateSize > peakStateSize) {
            peakStateSize = stateSize
          }
        }
      }
    } finally {
      query.stop()
    }

    val totalTime = System.currentTimeMillis() - startTime
    val totalEvents = NUM_EVENTS.toLong * NUM_BATCHES
    val eventsPerSecond = if (totalTime > 0) (totalEvents * 1000) / totalTime else 0

    BenchmarkResult(
      testName = queryName,
      providerName = getProviderNameFromQuery(spark),
      totalTimeMs = totalTime,
      avgBatchTimeMs = batchTimes.sum / batchTimes.size,
      eventsPerSecond = eventsPerSecond,
      peakStateSize = peakStateSize,
      batchTimes = batchTimes.toSeq
    )
  }

  private def runStreamingQueryUpdateMode[T](
      spark: SparkSession,
      memoryStream: MemoryStream[BenchmarkEvent],
      result: Dataset[T],
      checkpointDir: String): BenchmarkResult = {

    val batchTimes = new ArrayBuffer[Long]()
    val startTime = System.currentTimeMillis()
    var peakStateSize = 0L
    val queryName = "updateMode"

    val query = result.writeStream
      .format("memory")
      .queryName(queryName)
      .outputMode(OutputMode.Update())
      .option("checkpointLocation", s"$checkpointDir/$queryName")
      .trigger(Trigger.ProcessingTime(0))
      .start()

    try {
      for (batch <- 0 until NUM_BATCHES) {
        val batchStart = System.currentTimeMillis()

        val events = generateEvents(batch)
        memoryStream.addData(events)

        query.processAllAvailable()

        val batchTime = System.currentTimeMillis() - batchStart
        batchTimes += batchTime

        val progress = query.lastProgress
        if (progress != null && progress.stateOperators.nonEmpty) {
          val stateSize = progress.stateOperators.map(_.numRowsTotal).sum
          if (stateSize > peakStateSize) {
            peakStateSize = stateSize
          }
        }
      }
    } finally {
      query.stop()
    }

    val totalTime = System.currentTimeMillis() - startTime
    val totalEvents = NUM_EVENTS.toLong * NUM_BATCHES
    val eventsPerSecond = if (totalTime > 0) (totalEvents * 1000) / totalTime else 0

    BenchmarkResult(
      testName = queryName,
      providerName = getProviderNameFromQuery(spark),
      totalTimeMs = totalTime,
      avgBatchTimeMs = batchTimes.sum / batchTimes.size,
      eventsPerSecond = eventsPerSecond,
      peakStateSize = peakStateSize,
      batchTimes = batchTimes.toSeq
    )
  }

  private def generateEvents(batchNum: Int): Seq[BenchmarkEvent] = {
    val baseTime = System.currentTimeMillis() + (batchNum * 60000L)
    val eventTypes = Seq("click", "view", "purchase", "scroll", "hover")
    val random = new scala.util.Random(batchNum)

    (0 until NUM_EVENTS).map { _ =>
      BenchmarkEvent(
        userId = s"user_${random.nextInt(NUM_KEYS)}",
        eventType = eventTypes(random.nextInt(eventTypes.size)),
        value = random.nextInt(1000).toLong,
        timestamp = new Timestamp(baseTime + random.nextInt(59000))
      )
    }
  }

  private def generateHighCardinalityEvents(batchNum: Int, numKeys: Int): Seq[BenchmarkEvent] = {
    val baseTime = System.currentTimeMillis() + (batchNum * 60000L)
    val eventTypes = Seq("click", "view", "purchase", "scroll", "hover")
    val random = new scala.util.Random(batchNum)

    (0 until NUM_EVENTS).map { _ =>
      BenchmarkEvent(
        userId = s"user_${random.nextInt(numKeys)}",
        eventType = eventTypes(random.nextInt(eventTypes.size)),
        value = random.nextInt(1000).toLong,
        timestamp = new Timestamp(baseTime + random.nextInt(59000))
      )
    }
  }

  private def getProviderNameFromQuery(spark: SparkSession): String = {
    val providerClass = spark.conf.get("spark.sql.streaming.stateStore.providerClass")
    if (providerClass.contains("HDFS")) "HDFS"
    else if (providerClass.contains("RocksDB")) "RocksDB"
    else if (providerClass.contains("LSMTree")) "LSMTree"
    else "Unknown"
  }

  private def printResult(result: BenchmarkResult): Unit = {
    // scalastyle:off println
    println(s"  ${result.providerName}:")
    println(s"    Total Time:      ${result.totalTimeMs} ms")
    println(s"    Avg Batch Time:  ${result.avgBatchTimeMs} ms")
    println(s"    Events/sec:      ${result.eventsPerSecond}")
    println(s"    Peak State Size: ${result.peakStateSize} rows")
    println(s"    Batch Times:     ${result.batchTimes.mkString(", ")} ms")
    println()
    // scalastyle:on println
  }

  private def printComparison(testName: String): Unit = {
    // scalastyle:off caselocale
    val testResults = benchmarkResults.filter(_.testName ==
      testName.toLowerCase.replace(" ", "").replace("(", "").replace(")", ""))
    // scalastyle:on caselocale

    if (testResults.size >= 2) {
      // scalastyle:off println
      println(s"\n--- COMPARISON for $testName ---")

      // Find baseline (HDFS)
      val hdfsResult = testResults.find(_.providerName == "HDFS")
      val rocksResult = testResults.find(_.providerName == "RocksDB")
      val lsmResult = testResults.find(_.providerName == "LSMTree")

      hdfsResult.foreach { baseline =>
        println(s"\nRelative Performance (vs HDFS):")

        rocksResult.foreach { r =>
          val speedup = baseline.totalTimeMs.toDouble / r.totalTimeMs
          val status = if (speedup > 1) "FASTER" else "SLOWER"
          println(f"  RocksDB:  $speedup%.2fx $status")
        }

        lsmResult.foreach { r =>
          val speedup = baseline.totalTimeMs.toDouble / r.totalTimeMs
          val status = if (speedup > 1) "FASTER" else "SLOWER"
          println(f"  LSMTree:  $speedup%.2fx $status")
        }
      }

      // Throughput comparison
      println(s"\nThroughput (events/sec):")
      testResults.sortBy(-_.eventsPerSecond).foreach { r =>
        println(f"  ${r.providerName}%-10s: ${r.eventsPerSecond}%,d")
      }

      println()
      // scalastyle:on println
    }
  }

  override def afterAll(): Unit = {
    printFinalSummary()
    super.afterAll()
  }

  private def printFinalSummary(): Unit = {
    if (benchmarkResults.isEmpty) return

    // scalastyle:off println
    println("\n" + "=" * 80)
    println("FINAL BENCHMARK SUMMARY")
    println("=" * 80)

    // Group by provider
    val byProvider = benchmarkResults.groupBy(_.providerName)

    println("\nAverage Performance Across All Tests:")
    println("-" * 50)

    byProvider.foreach { case (provider, results) =>
      val avgTime = results.map(_.totalTimeMs).sum / results.size
      val avgThroughput = results.map(_.eventsPerSecond).sum / results.size
      val avgBatchTime = results.map(_.avgBatchTimeMs).sum / results.size

      println(f"$provider%-10s:")
      println(f"  Avg Total Time:    $avgTime%,d ms")
      println(f"  Avg Batch Time:    $avgBatchTime%,d ms")
      println(f"  Avg Throughput:    $avgThroughput%,d events/sec")
      println()
    }

    // Best statestore determination
    val avgThroughputs = byProvider.map { case (provider, results) =>
      provider -> (results.map(_.eventsPerSecond).sum / results.size)
    }

    val best = avgThroughputs.maxBy(_._2)
    println(s" Best: ${best._1} with ${best._2} avg events/sec")
    println("=" * 80)
    // scalastyle:on println
  }
}

