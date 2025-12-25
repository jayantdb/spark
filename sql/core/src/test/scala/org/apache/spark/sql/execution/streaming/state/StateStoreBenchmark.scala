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

package org.apache.spark.sql.execution.streaming.state

import java.io.File
import java.util.UUID

import scala.util.Random

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.runtime.StreamExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Benchmark comparing StateStore implementations:
 * - HDFSBackedStateStoreProvider (baseline)
 * - RocksDBStateStoreProvider (current production choice)
 * - LSMTreeStateStoreProvider (new pure-Scala implementation)
 *
 * Run with:
 * {{{
 *   build/sbt "sql/testOnly *StateStoreBenchmark"
 * }}}
 *
 * Or generate benchmark results:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/testOnly *StateStoreBenchmark"
 * }}}
 */
class StateStoreBenchmark extends SparkFunSuite {

  private val keySchema = new StructType().add("key", LongType)
  private val valueSchema = new StructType().add("value", StringType)

  private val keyProjection = UnsafeProjection.create(keySchema)
  private val valueProjection = UnsafeProjection.create(valueSchema)

  private def createKeyRow(key: Long): UnsafeRow = {
    keyProjection.apply(new GenericInternalRow(Array[Any](key)))
  }

  private def createValueRow(value: String): UnsafeRow = {
    valueProjection.apply(new GenericInternalRow(Array[Any](
      UTF8String.fromString(value)
    )))
  }

  private def createProvider(
      providerClass: Class[_ <: StateStoreProvider],
      tempDir: File): StateStoreProvider = {
    val storeId = StateStoreId(
      checkpointRootLocation = tempDir.getAbsolutePath,
      operatorId = 0,
      partitionId = 0
    )

    // Use SQLConf to set the provider class (the proper Spark way)
    val sqlConf = new SQLConf()
    sqlConf.setConfString(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      providerClass.getName
    )

    val storeConf = new StateStoreConf(sqlConf)
    val hadoopConf = new Configuration()
    hadoopConf.set(StreamExecution.RUN_ID_KEY, UUID.randomUUID().toString)

    // Create provider using StateStoreProvider.create which respects the config
    val provider = StateStoreProvider.create(storeConf.providerClass)
    provider.init(
      stateStoreId = storeId,
      keySchema = keySchema,
      valueSchema = valueSchema,
      keyStateEncoderSpec = NoPrefixKeyStateEncoderSpec(keySchema),
      useColumnFamilies = false,
      storeConfs = storeConf,
      hadoopConf = hadoopConf
    )
    provider
  }

  test("Benchmark: Write Throughput (100K keys)") {
    val numKeys = 100000
    val valueSize = 100

    // scalastyle:off println
    println("\n" + "=" * 80)
    println("BENCHMARK: Write Throughput (100K keys)")
    println("=" * 80)

    val results = Seq(
      ("HDFS", classOf[HDFSBackedStateStoreProvider]),
      ("RocksDB", classOf[RocksDBStateStoreProvider]),
      ("LSMTree", classOf[LSMTreeStateStoreProvider])
    ).map { case (name, providerClass) =>
      val (timeMs, opsPerSec) = benchmarkWrites(providerClass, numKeys, valueSize)
      println(f"$name%-10s: $timeMs%8.2f ms, $opsPerSec%12.0f ops/sec")
      (name, timeMs, opsPerSec)
    }

    printComparisonTable("Write Throughput", results)
    // scalastyle:on println
  }

  test("Benchmark: Read Throughput (100K keys)") {
    val numKeys = 100000
    val valueSize = 100

    // scalastyle:off println
    println("\n" + "=" * 80)
    println("BENCHMARK: Read Throughput (100K keys)")
    println("=" * 80)

    val results = Seq(
      ("HDFS", classOf[HDFSBackedStateStoreProvider]),
      ("RocksDB", classOf[RocksDBStateStoreProvider]),
      ("LSMTree", classOf[LSMTreeStateStoreProvider])
    ).map { case (name, providerClass) =>
      val (timeMs, opsPerSec) = benchmarkReads(providerClass, numKeys, valueSize)
      println(f"$name%-10s: $timeMs%8.2f ms, $opsPerSec%12.0f ops/sec")
      (name, timeMs, opsPerSec)
    }

    printComparisonTable("Read Throughput", results)
    // scalastyle:on println
  }

  test("Benchmark: Mixed Workload (50% reads, 50% writes)") {
    val numOperations = 100000
    val valueSize = 100

    // scalastyle:off println
    println("\n" + "=" * 80)
    println("BENCHMARK: Mixed Workload (50% reads, 50% writes)")
    println("=" * 80)

    val results = Seq(
      ("HDFS", classOf[HDFSBackedStateStoreProvider]),
      ("RocksDB", classOf[RocksDBStateStoreProvider]),
      ("LSMTree", classOf[LSMTreeStateStoreProvider])
    ).map { case (name, providerClass) =>
      val (timeMs, opsPerSec) = benchmarkMixed(providerClass, numOperations, valueSize)
      println(f"$name%-10s: $timeMs%8.2f ms, $opsPerSec%12.0f ops/sec")
      (name, timeMs, opsPerSec)
    }

    printComparisonTable("Mixed Workload", results)
    // scalastyle:on println
  }

  test("Benchmark: Iterator Performance (100K keys)") {
    val numKeys = 100000
    val valueSize = 100

    // scalastyle:off println
    println("\n" + "=" * 80)
    println("BENCHMARK: Iterator Performance (100K keys)")
    println("=" * 80)

    val results = Seq(
      ("HDFS", classOf[HDFSBackedStateStoreProvider]),
      ("RocksDB", classOf[RocksDBStateStoreProvider]),
      ("LSMTree", classOf[LSMTreeStateStoreProvider])
    ).map { case (name, providerClass) =>
      val (timeMs, opsPerSec) = benchmarkIterator(providerClass, numKeys, valueSize)
      println(f"$name%-10s: $timeMs%8.2f ms, $opsPerSec%12.0f keys/sec")
      (name, timeMs, opsPerSec)
    }

    printComparisonTable("Iterator", results)
    // scalastyle:on println
  }

  test("Benchmark: Large Values (10K keys, 10KB values)") {
    val numKeys = 10000
    val valueSize = 10 * 1024  // 10 KB

    // scalastyle:off println
    println("\n" + "=" * 80)
    println("BENCHMARK: Large Values (10K keys, 10KB values)")
    println("=" * 80)

    val results = Seq(
      ("HDFS", classOf[HDFSBackedStateStoreProvider]),
      ("RocksDB", classOf[RocksDBStateStoreProvider]),
      ("LSMTree", classOf[LSMTreeStateStoreProvider])
    ).map { case (name, providerClass) =>
      val (timeMs, opsPerSec) = benchmarkWrites(providerClass, numKeys, valueSize)
      println(f"$name%-10s: $timeMs%8.2f ms, $opsPerSec%12.0f ops/sec")
      (name, timeMs, opsPerSec)
    }

    printComparisonTable("Large Values", results)
    // scalastyle:on println
  }

  test("Benchmark: Point Lookups (random reads from 100K keys)") {
    val numKeys = 100000
    val numLookups = 50000
    val valueSize = 100

    // scalastyle:off println
    println("\n" + "=" * 80)
    println("BENCHMARK: Point Lookups (50K random reads from 100K keys)")
    println("=" * 80)

    val results = Seq(
      ("HDFS", classOf[HDFSBackedStateStoreProvider]),
      ("RocksDB", classOf[RocksDBStateStoreProvider]),
      ("LSMTree", classOf[LSMTreeStateStoreProvider])
    ).map { case (name, providerClass) =>
      val (timeMs, opsPerSec) = benchmarkPointLookups(providerClass, numKeys, numLookups, valueSize)
      println(f"$name%-10s: $timeMs%8.2f ms, $opsPerSec%12.0f ops/sec")
      (name, timeMs, opsPerSec)
    }

    printComparisonTable("Point Lookups", results)
    // scalastyle:on println
  }

  test("Benchmark: Commit Performance (with 10K pending changes)") {
    val numKeys = 10000
    val valueSize = 100

    // scalastyle:off println
    println("\n" + "=" * 80)
    println("BENCHMARK: Commit Performance (10K pending changes)")
    println("=" * 80)

    val results = Seq(
      ("HDFS", classOf[HDFSBackedStateStoreProvider]),
      ("RocksDB", classOf[RocksDBStateStoreProvider]),
      ("LSMTree", classOf[LSMTreeStateStoreProvider])
    ).map { case (name, providerClass) =>
      val (timeMs, _) = benchmarkCommit(providerClass, numKeys, valueSize)
      println(f"$name%-10s: $timeMs%8.2f ms")
      (name, timeMs, 1000.0 / timeMs)
    }

    printComparisonTable("Commit", results)
    // scalastyle:on println
  }

  // ============================================================================
  // Benchmark Implementations
  // ============================================================================

  private def benchmarkWrites(
      providerClass: Class[_ <: StateStoreProvider],
      numKeys: Int,
      valueSize: Int): (Double, Double) = {

    val value = "x" * valueSize

    withProviderTimed(providerClass) { provider =>
      val store = provider.getStore(0)
      try {
        val startTime = System.nanoTime()

        for (i <- 0 until numKeys) {
          store.put(createKeyRow(i), createValueRow(value))
        }

        val endTime = System.nanoTime()
        val timeMs = (endTime - startTime) / 1e6
        val opsPerSec = numKeys / (timeMs / 1000.0)

        store.commit()
        (timeMs, opsPerSec)
      } catch {
        case e: Throwable =>
          store.abort()
          throw e
      }
    }
  }

  private def benchmarkReads(
      providerClass: Class[_ <: StateStoreProvider],
      numKeys: Int,
      valueSize: Int): (Double, Double) = {

    val value = "x" * valueSize

    withProviderTimed(providerClass) { provider =>
      // First, populate the store
      val store1 = provider.getStore(0)
      for (i <- 0 until numKeys) {
        store1.put(createKeyRow(i), createValueRow(value))
      }
      store1.commit()

      // Now benchmark reads
      val store2 = provider.getStore(1)
      try {
        val startTime = System.nanoTime()

        for (i <- 0 until numKeys) {
          store2.get(createKeyRow(i))
        }

        val endTime = System.nanoTime()
        val timeMs = (endTime - startTime) / 1e6
        val opsPerSec = numKeys / (timeMs / 1000.0)

        store2.commit()
        (timeMs, opsPerSec)
      } catch {
        case e: Throwable =>
          store2.abort()
          throw e
      }
    }
  }

  private def benchmarkMixed(
      providerClass: Class[_ <: StateStoreProvider],
      numOperations: Int,
      valueSize: Int): (Double, Double) = {

    val value = "x" * valueSize
    val random = new Random(42)

    withProviderTimed(providerClass) { provider =>
      // Pre-populate with half the keys
      val store1 = provider.getStore(0)
      for (i <- 0 until numOperations / 2) {
        store1.put(createKeyRow(i), createValueRow(value))
      }
      store1.commit()

      // Mixed workload
      val store2 = provider.getStore(1)
      try {
        val startTime = System.nanoTime()

        for (_ <- 0 until numOperations) {
          val key = random.nextInt(numOperations)
          if (random.nextBoolean()) {
            store2.put(createKeyRow(key), createValueRow(value))
          } else {
            store2.get(createKeyRow(key))
          }
        }

        val endTime = System.nanoTime()
        val timeMs = (endTime - startTime) / 1e6
        val opsPerSec = numOperations / (timeMs / 1000.0)

        store2.commit()
        (timeMs, opsPerSec)
      } catch {
        case e: Throwable =>
          store2.abort()
          throw e
      }
    }
  }

  private def benchmarkIterator(
      providerClass: Class[_ <: StateStoreProvider],
      numKeys: Int,
      valueSize: Int): (Double, Double) = {

    val value = "x" * valueSize

    withProviderTimed(providerClass) { provider =>
      // Populate the store
      val store1 = provider.getStore(0)
      for (i <- 0 until numKeys) {
        store1.put(createKeyRow(i), createValueRow(value))
      }
      store1.commit()

      // Benchmark iteration
      val store2 = provider.getStore(1)
      try {
        val startTime = System.nanoTime()

        var count = 0
        val iter = store2.iterator()
        while (iter.hasNext) {
          iter.next()
          count += 1
        }

        val endTime = System.nanoTime()
        val timeMs = (endTime - startTime) / 1e6
        val keysPerSec = count / (timeMs / 1000.0)

        store2.commit()
        (timeMs, keysPerSec)
      } catch {
        case e: Throwable =>
          store2.abort()
          throw e
      }
    }
  }

  private def benchmarkPointLookups(
      providerClass: Class[_ <: StateStoreProvider],
      numKeys: Int,
      numLookups: Int,
      valueSize: Int): (Double, Double) = {

    val value = "x" * valueSize
    val random = new Random(42)
    val lookupKeys = Array.fill(numLookups)(random.nextInt(numKeys).toLong)

    withProviderTimed(providerClass) { provider =>
      // Populate the store
      val store1 = provider.getStore(0)
      for (i <- 0 until numKeys) {
        store1.put(createKeyRow(i), createValueRow(value))
      }
      store1.commit()

      // Benchmark random lookups
      val store2 = provider.getStore(1)
      try {
        val startTime = System.nanoTime()

        for (key <- lookupKeys) {
          store2.get(createKeyRow(key))
        }

        val endTime = System.nanoTime()
        val timeMs = (endTime - startTime) / 1e6
        val opsPerSec = numLookups / (timeMs / 1000.0)

        store2.commit()
        (timeMs, opsPerSec)
      } catch {
        case e: Throwable =>
          store2.abort()
          throw e
      }
    }
  }

  private def benchmarkCommit(
      providerClass: Class[_ <: StateStoreProvider],
      numKeys: Int,
      valueSize: Int): (Double, Double) = {

    val value = "x" * valueSize

    withProviderTimed(providerClass) { provider =>
      val store = provider.getStore(0)
      try {
        // Add pending changes
        for (i <- 0 until numKeys) {
          store.put(createKeyRow(i), createValueRow(value))
        }

        // Benchmark commit
        val startTime = System.nanoTime()
        store.commit()
        val endTime = System.nanoTime()

        val timeMs = (endTime - startTime) / 1e6
        (timeMs, 0.0)
      } catch {
        case e: Throwable =>
          store.abort()
          throw e
      }
    }
  }

  private def withProviderTimed[T](
      providerClass: Class[_ <: StateStoreProvider])(
      f: StateStoreProvider => T): T = {
    val tempDir = Utils.createTempDir()
    val provider = createProvider(providerClass, tempDir)
    try {
      f(provider)
    } finally {
      provider.close()
      Utils.deleteRecursively(tempDir)
    }
  }

  // scalastyle:off println
  private def printComparisonTable(
      testName: String,
      results: Seq[(String, Double, Double)]): Unit = {

    println("\n" + "-" * 60)
    println(f"$testName Comparison (normalized to HDFS baseline):")
    println("-" * 60)

    val hdfsOps = results.find(_._1 == "HDFS").map(_._3).getOrElse(1.0)

    results.foreach { case (name, _, opsPerSec) =>
      val speedup = opsPerSec / hdfsOps
      val speedupStr =
        if (speedup >= 1.0) f"+${(speedup - 1) * 100}%.0f%%"
        else f"${(speedup - 1) * 100}%.0f%%"
      println(f"  $name%-10s: $speedup%6.2fx faster ($speedupStr)")
    }
    println()
  }
  // scalastyle:on println
}
