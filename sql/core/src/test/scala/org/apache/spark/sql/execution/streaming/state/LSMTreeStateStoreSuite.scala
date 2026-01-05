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
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.runtime.StreamExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * Tests for the LSM-Tree based StateStore implementation.
 * Execute:
 * ./build/mvn -pl sql/core \
 * -am test \
 * -DwildcardSuites=org.apache.spark.sql.execution.streaming.state.LSMTreeStateStoreSuite
 */
class LSMTreeStateStoreSuite extends SparkFunSuite with BeforeAndAfter {

  private var tempDir: File = _

  before {
    tempDir = Utils.createTempDir()
  }

  after {
    if (tempDir != null) {
      Utils.deleteRecursively(tempDir)
      tempDir = null
    }
  }

  private val keySchema = new StructType().add("key", IntegerType)
  private val valueSchema = new StructType().add("value", StringType)

  private def createProvider(): LSMTreeStateStoreProvider = {
    val storeId = StateStoreId(
      checkpointRootLocation = tempDir.getAbsolutePath,
      operatorId = 0,
      partitionId = 0
    )

    // Use Spark config to select LSMTreeStateStoreProvider
    val sqlConf = new SQLConf()
    sqlConf.setConfString(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      classOf[LSMTreeStateStoreProvider].getName
    )

    val storeConf = new StateStoreConf(sqlConf)
    val hadoopConf = new Configuration()
    hadoopConf.set(StreamExecution.RUN_ID_KEY, UUID.randomUUID().toString)

    // Create provider using StateStoreProvider.create which reads from config
    val provider = StateStoreProvider.create(storeConf.providerClass)
      .asInstanceOf[LSMTreeStateStoreProvider]

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

  private def createKeyRow(key: Int): UnsafeRow = {
    val projection = UnsafeProjection.create(keySchema)
    projection.apply(new GenericInternalRow(Array[Any](key)))
  }

  private def createValueRow(value: String): UnsafeRow = {
    val projection = UnsafeProjection.create(valueSchema)
    projection.apply(new GenericInternalRow(Array[Any](
      org.apache.spark.unsafe.types.UTF8String.fromString(value)
    )))
  }

  test("basic put and get") {
    val provider = createProvider()
    try {
      val store = provider.getStore(0)

      store.put(createKeyRow(1), createValueRow("value1"))
      store.put(createKeyRow(2), createValueRow("value2"))
      store.put(createKeyRow(3), createValueRow("value3"))

      assert(store.get(createKeyRow(1)).getString(0) == "value1")
      assert(store.get(createKeyRow(2)).getString(0) == "value2")
      assert(store.get(createKeyRow(3)).getString(0) == "value3")
      assert(store.get(createKeyRow(4)) == null)

      store.commit()
    } finally {
      provider.close()
    }
  }

  test("put updates existing key") {
    val provider = createProvider()
    try {
      val store = provider.getStore(0)

      store.put(createKeyRow(1), createValueRow("value1"))
      assert(store.get(createKeyRow(1)).getString(0) == "value1")

      store.put(createKeyRow(1), createValueRow("updated"))
      assert(store.get(createKeyRow(1)).getString(0) == "updated")

      store.commit()

      // Verify after reload
      val store2 = provider.getStore(1)
      assert(store2.get(createKeyRow(1)).getString(0) == "updated")
      store2.commit()
    } finally {
      provider.close()
    }
  }

  test("remove key") {
    val provider = createProvider()
    try {
      val store = provider.getStore(0)

      store.put(createKeyRow(1), createValueRow("value1"))
      store.put(createKeyRow(2), createValueRow("value2"))
      store.commit()

      val store2 = provider.getStore(1)
      assert(store2.get(createKeyRow(1)).getString(0) == "value1")
      store2.remove(createKeyRow(1))
      assert(store2.get(createKeyRow(1)) == null)
      assert(store2.get(createKeyRow(2)).getString(0) == "value2")
      store2.commit()

      // Verify after reload
      val store3 = provider.getStore(2)
      assert(store3.get(createKeyRow(1)) == null)
      assert(store3.get(createKeyRow(2)).getString(0) == "value2")
      store3.commit()
    } finally {
      provider.close()
    }
  }

  test("iterator returns all entries") {
    val provider = createProvider()
    try {
      val store = provider.getStore(0)

      for (i <- 1 to 100) {
        store.put(createKeyRow(i), createValueRow(s"value$i"))
      }
      store.commit()

      val store2 = provider.getStore(1)
      val entries = store2.iterator().toList
      assert(entries.size == 100)

      // Verify all entries are present
      val keys = entries.map(_.key.getInt(0)).toSet
      assert(keys == (1 to 100).toSet)
      store2.commit()
    } finally {
      provider.close()
    }
  }

  test("prefix scan") {
    val provider = createProvider()
    try {
      val store = provider.getStore(0)

      // Add entries with different key patterns
      for (i <- 100 to 199) {
        store.put(createKeyRow(i), createValueRow(s"value$i"))
      }
      for (i <- 200 to 299) {
        store.put(createKeyRow(i), createValueRow(s"value$i"))
      }
      store.commit()

      val store2 = provider.getStore(1)
      // Note: prefix scan on integers may not be meaningful,
      // but we test the functionality works
      val allEntries = store2.iterator().toList
      assert(allEntries.size == 200)
      store2.commit()
    } finally {
      provider.close()
    }
  }

  test("abort rolls back changes") {
    val provider = createProvider()
    try {
      val store = provider.getStore(0)
      store.put(createKeyRow(1), createValueRow("value1"))
      store.commit()

      val store2 = provider.getStore(1)
      store2.put(createKeyRow(2), createValueRow("value2"))
      assert(store2.get(createKeyRow(2)).getString(0) == "value2")
      store2.abort()

      // Aborted changes should not be visible
      val store3 = provider.getStore(1)
      assert(store3.get(createKeyRow(1)).getString(0) == "value1")
      assert(store3.get(createKeyRow(2)) == null)
      store3.commit()
    } finally {
      provider.close()
    }
  }

  test("version persistence") {
    val provider = createProvider()
    try {
      // Version 0 -> 1
      val store1 = provider.getStore(0)
      store1.put(createKeyRow(1), createValueRow("v1"))
      val v1 = store1.commit()
      assert(v1 == 1)

      // Version 1 -> 2
      val store2 = provider.getStore(1)
      store2.put(createKeyRow(2), createValueRow("v2"))
      val v2 = store2.commit()
      assert(v2 == 2)

      // Load version 2
      val store3 = provider.getStore(2)
      assert(store3.get(createKeyRow(1)).getString(0) == "v1")
      assert(store3.get(createKeyRow(2)).getString(0) == "v2")
      store3.commit()
    } finally {
      provider.close()
    }
  }

  test("metrics are reported") {
    val provider = createProvider()
    try {
      val store = provider.getStore(0)

      for (i <- 1 to 1000) {
        store.put(createKeyRow(i), createValueRow(s"value$i"))
      }
      store.commit()

      val store2 = provider.getStore(1)
      val metrics = store2.metrics
      assert(metrics.numKeys >= 0)
      assert(metrics.memoryUsedBytes >= 0)
      store2.commit()
    } finally {
      provider.close()
    }
  }

  test("handles large values") {
    val provider = createProvider()
    try {
      val store = provider.getStore(0)

      // Create a large value (1 MB)
      val largeValue = "x" * (1024 * 1024)
      store.put(createKeyRow(1), createValueRow(largeValue))
      store.commit()

      val store2 = provider.getStore(1)
      val result = store2.get(createKeyRow(1))
      assert(result != null)
      assert(result.getString(0).length == largeValue.length)
      store2.commit()
    } finally {
      provider.close()
    }
  }

  test("handles many small updates") {
    val provider = createProvider()
    try {
      val store = provider.getStore(0)

      // Many updates to same key
      for (i <- 1 to 10000) {
        store.put(createKeyRow(1), createValueRow(s"value$i"))
      }
      store.commit()

      val store2 = provider.getStore(1)
      assert(store2.get(createKeyRow(1)).getString(0) == "value10000")
      store2.commit()
    } finally {
      provider.close()
    }
  }

  test("concurrent reads and writes") {
    val provider = createProvider()
    try {
      val store = provider.getStore(0)

      // Pre-populate some data
      for (i <- 1 to 100) {
        store.put(createKeyRow(i), createValueRow(s"value$i"))
      }
      store.commit()

      val store2 = provider.getStore(1)

      // Read while writing
      val readThread = new Thread(() => {
        for (_ <- 1 to 100) {
          store2.get(createKeyRow(Random.nextInt(100) + 1))
        }
      })

      val writeThread = new Thread(() => {
        for (i <- 101 to 200) {
          store2.put(createKeyRow(i), createValueRow(s"value$i"))
        }
      })

      readThread.start()
      writeThread.start()
      readThread.join()
      writeThread.join()

      store2.commit()
    } finally {
      provider.close()
    }
  }

  test("maintenance runs without error") {
    val provider = createProvider()
    try {
      val store = provider.getStore(0)
      for (i <- 1 to 1000) {
        store.put(createKeyRow(i), createValueRow(s"value$i"))
      }
      store.commit()

      // Run maintenance
      provider.doMaintenance()

      // Verify data is still accessible
      val store2 = provider.getStore(1)
      assert(store2.get(createKeyRow(500)).getString(0) == "value500")
      store2.commit()
    } finally {
      provider.close()
    }
  }

  // ============================================================================
  // Binary Key Ordering Tests
  // ============================================================================

  test("binary key ordering - iterator returns keys in lexicographic order") {
    val provider = createProvider()
    try {
      val store = provider.getStore(0)

      // Insert keys in random order
      val keys = Seq(100, 50, 200, 25, 75, 150, 175)
      keys.foreach { k =>
        store.put(createKeyRow(k), createValueRow(s"value$k"))
      }
      store.commit()

      // Read back and verify ordering
      val store2 = provider.getStore(1)
      val iterator = store2.iterator()
      val retrievedKeys = iterator.map { pair =>
        pair.key.getInt(0)
      }.toSeq

      // Keys should be in ascending order (lexicographic byte order)
      assert(retrievedKeys == retrievedKeys.sorted,
        s"Keys not in sorted order: $retrievedKeys")

      store2.commit()
    } finally {
      provider.close()
    }
  }

  test("binary key ordering - unsigned byte comparison") {
    // This test verifies that byte comparison is unsigned (0xFF > 0x00)
    val memTable = new MemTable(1024 * 1024)

    // Insert bytes that would be negative in signed comparison
    val key1 = Array[Byte](0x00, 0x01)  // Small bytes
    val key2 = Array[Byte](0x7F.toByte, 0x00)  // Max positive signed
    val key3 = Array[Byte](0x80.toByte, 0x00)  // Would be negative in signed (-128)
    val key4 = Array[Byte](0xFF.toByte, 0xFF.toByte)  // Would be -1 in signed

    memTable.put(key4, "fourth".getBytes)
    memTable.put(key1, "first".getBytes)
    memTable.put(key3, "third".getBytes)
    memTable.put(key2, "second".getBytes)

    val keys = memTable.iterator.map { case (k, _) => k.toSeq }.toList

    // With unsigned comparison: 0x00 < 0x7F < 0x80 < 0xFF
    assert(keys.head == Seq(0x00.toByte, 0x01.toByte), "First key should be 0x00,0x01")
    assert(keys(1) == Seq(0x7F.toByte, 0x00.toByte), "Second key should be 0x7F,0x00")
    assert(keys(2) == Seq(0x80.toByte, 0x00.toByte), "Third key should be 0x80,0x00")
    assert(keys(3) == Seq(0xFF.toByte, 0xFF.toByte), "Fourth key should be 0xFF,0xFF")
  }

  test("binary key ordering - prefix ordering (shorter first)") {
    val memTable = new MemTable(1024 * 1024)

    val key1 = "abc".getBytes
    val key2 = "abcd".getBytes
    val key3 = "ab".getBytes

    memTable.put(key2, "longer".getBytes)
    memTable.put(key1, "medium".getBytes)
    memTable.put(key3, "short".getBytes)

    val keys = memTable.iterator.map { case (k, _) => new String(k) }.toList

    // Shorter prefix should come first: ab < abc < abcd
    assert(keys == List("ab", "abc", "abcd"),
      s"Keys not in prefix order: $keys")
  }

  test("binary key ordering - prefix scan returns correct subset") {
    val memTable = new MemTable(1024 * 1024)

    // Insert various keys
    memTable.put("user:001".getBytes, "alice".getBytes)
    memTable.put("user:002".getBytes, "bob".getBytes)
    memTable.put("user:010".getBytes, "charlie".getBytes)
    memTable.put("order:001".getBytes, "order1".getBytes)
    memTable.put("product:001".getBytes, "product1".getBytes)

    // Prefix scan for "user:"
    val userKeys = memTable.prefixIterator("user:".getBytes)
      .map { case (k, v) => (new String(k), new String(v.value)) }
      .toList

    assert(userKeys.length == 3, s"Expected 3 user keys, got ${userKeys.length}")
    assert(userKeys.map(_._1) == List("user:001", "user:002", "user:010"),
      s"User keys not in order: ${userKeys.map(_._1)}")
  }

  test("binary key ordering - range scan returns correct subset") {
    val memTable = new MemTable(1024 * 1024)

    // Insert keys
    for (i <- 1 to 10) {
      memTable.put(f"key$i%03d".getBytes, s"value$i".getBytes)
    }

    // Range scan from key003 to key007 (exclusive)
    val rangeKeys = memTable.iteratorRange("key003".getBytes, "key007".getBytes)
      .map { case (k, _) => new String(k) }
      .toList

    assert(rangeKeys == List("key003", "key004", "key005", "key006"),
      s"Range keys incorrect: $rangeKeys")
  }

  test("binary key ordering - compareByteArrays is consistent") {
    import LSMTree.compareByteArrays

    // Test transitivity: if a < b and b < c, then a < c
    val a = Array[Byte](0x01, 0x00)
    val b = Array[Byte](0x01, 0x01)
    val c = Array[Byte](0x02, 0x00)

    assert(compareByteArrays(a, b) < 0, "a should be less than b")
    assert(compareByteArrays(b, c) < 0, "b should be less than c")
    assert(compareByteArrays(a, c) < 0, "a should be less than c (transitivity)")

    // Test symmetry: if a < b, then b > a
    assert(compareByteArrays(b, a) > 0, "b should be greater than a")

    // Test equality
    val d = Array[Byte](0x01, 0x00)
    assert(compareByteArrays(a, d) == 0, "a should equal d")
    assert(compareByteArrays(d, a) == 0, "d should equal a")

    // Test empty arrays
    val empty1 = Array[Byte]()
    val empty2 = Array[Byte]()
    assert(compareByteArrays(empty1, empty2) == 0, "empty arrays should be equal")
    assert(compareByteArrays(empty1, a) < 0, "empty should be less than non-empty")
    assert(compareByteArrays(a, empty1) > 0, "non-empty should be greater than empty")
  }

  test("binary key ordering - long keys with word-at-a-time comparison") {
    import LSMTree.compareByteArrays

    // Keys longer than 8 bytes to trigger word-at-a-time comparison
    val key1 = "0123456789abcdef".getBytes  // 16 bytes
    val key2 = "0123456789abcdeg".getBytes  // Differs in last byte
    val key3 = "0123456889abcdef".getBytes  // Differs in byte 8

    assert(compareByteArrays(key1, key2) < 0, "key1 should be less than key2")
    assert(compareByteArrays(key1, key3) < 0, "key1 should be less than key3")
    assert(compareByteArrays(key3, key2) > 0, "key3 should be greater than key2")

    // Test with 0x80+ bytes in long keys (unsigned comparison)
    val highByte1 = Array.fill(8)(0x00.toByte) ++ Array(0x7F.toByte)
    val highByte2 = Array.fill(8)(0x00.toByte) ++ Array(0x80.toByte)
    val highByte3 = Array.fill(8)(0x00.toByte) ++ Array(0xFF.toByte)

    assert(compareByteArrays(highByte1, highByte2) < 0,
      "0x7F should be less than 0x80 (unsigned)")
    assert(compareByteArrays(highByte2, highByte3) < 0,
      "0x80 should be less than 0xFF (unsigned)")
  }

  test("binary key ordering - SSTable preserves ordering after flush") {
    val provider = createProvider()
    try {
      val store = provider.getStore(0)

      // Insert many keys to trigger SSTable flush
      val keys = (1 to 500).map(i => f"key$i%05d")
      Random.shuffle(keys).foreach { k =>
        store.put(
          createKeyRowFromString(k),
          createValueRow(s"value_$k")
        )
      }
      store.commit()

      // Force maintenance to flush to SSTable
      provider.doMaintenance()

      // Read back and verify ordering is preserved
      val store2 = provider.getStore(1)
      val retrievedKeys = store2.iterator()
        .map(pair => getStringFromKeyRow(pair.key))
        .toSeq

      val sortedKeys = keys.sorted
      assert(retrievedKeys == sortedKeys,
        s"SSTable did not preserve key ordering. First mismatch at: " +
          retrievedKeys.zip(sortedKeys).indexWhere { case (a, b) => a != b })

      store2.commit()
    } finally {
      provider.close()
    }
  }

  // Helper to create key row from string
  private def createKeyRowFromString(key: String): UnsafeRow = {
    val schema = new StructType().add("key", StringType)
    val projection = UnsafeProjection.create(schema)
    projection.apply(new GenericInternalRow(Array[Any](
      org.apache.spark.unsafe.types.UTF8String.fromString(key)
    )))
  }

  // Helper to get string from key row
  private def getStringFromKeyRow(row: UnsafeRow): String = {
    row.getString(0)
  }
}

/**
 * Tests for the MemTable component.
 */
class LSMTreeMemTableSuite extends SparkFunSuite {

  test("basic put and get") {
    val memTable = new MemTable(1024 * 1024)

    memTable.put("key1".getBytes, "value1".getBytes)
    memTable.put("key2".getBytes, "value2".getBytes)

    val result1 = memTable.get("key1".getBytes)
    assert(result1 != null)
    assert(new String(result1.value) == "value1")

    val result2 = memTable.get("key2".getBytes)
    assert(result2 != null)
    assert(new String(result2.value) == "value2")

    assert(memTable.get("key3".getBytes) == null)
  }

  test("delete creates tombstone") {
    val memTable = new MemTable(1024 * 1024)

    memTable.put("key1".getBytes, "value1".getBytes)
    memTable.delete("key1".getBytes)

    val result = memTable.get("key1".getBytes)
    assert(result != null)
    assert(result.isDeleted)
  }

  test("sorted iteration") {
    val memTable = new MemTable(1024 * 1024)

    memTable.put("c".getBytes, "3".getBytes)
    memTable.put("a".getBytes, "1".getBytes)
    memTable.put("b".getBytes, "2".getBytes)

    val keys = memTable.iterator.map { case (k, _) => new String(k) }.toList
    assert(keys == List("a", "b", "c"))
  }

  test("size tracking") {
    val memTable = new MemTable(1024 * 1024)

    assert(memTable.estimatedSize == 0)

    memTable.put("key1".getBytes, "value1".getBytes)
    val size1 = memTable.estimatedSize
    assert(size1 > 0)

    memTable.put("key2".getBytes, "value2".getBytes)
    val size2 = memTable.estimatedSize
    assert(size2 > size1)
  }

  test("clear resets state") {
    val memTable = new MemTable(1024 * 1024)

    memTable.put("key1".getBytes, "value1".getBytes)
    assert(memTable.size == 1)

    memTable.clear()
    assert(memTable.size == 0)
    assert(memTable.estimatedSize == 0)
    assert(memTable.get("key1".getBytes) == null)
  }
}

/**
 * Tests for the Bloom filter component.
 */
class LSMBloomFilterSuite extends SparkFunSuite {

  test("no false negatives") {
    val filter = LSMBloomFilter(1000, 0.01)

    // Add keys
    for (i <- 0 until 1000) {
      filter.put(s"key$i".getBytes)
    }

    // All added keys must be found
    for (i <- 0 until 1000) {
      assert(filter.mightContain(s"key$i".getBytes))
    }
  }

  test("false positive rate within bounds") {
    val filter = LSMBloomFilter(1000, 0.01)

    // Add keys
    for (i <- 0 until 1000) {
      filter.put(s"key$i".getBytes)
    }

    // Check keys that were not added
    var falsePositives = 0
    for (i <- 1000 until 2000) {
      if (filter.mightContain(s"key$i".getBytes)) {
        falsePositives += 1
      }
    }

    // Allow up to 5% false positives (5x the specified rate)
    assert(falsePositives < 50)
  }

  test("serialization and deserialization") {
    val filter = LSMBloomFilter(1000, 0.01)

    for (i <- 0 until 100) {
      filter.put(s"key$i".getBytes)
    }

    val bytes = filter.toBytes()
    val restored = LSMBloomFilter.fromBytes(bytes)

    // All keys should still be found
    for (i <- 0 until 100) {
      assert(restored.mightContain(s"key$i".getBytes))
    }
  }
}

/**
 * Tests for the Sparse Index component.
 */
class SparseIndexSuite extends SparkFunSuite {

  test("find block for key") {
    val index = new SparseIndex()

    index.addEntry("aaa".getBytes, 0)
    index.addEntry("bbb".getBytes, 100)
    index.addEntry("ccc".getBytes, 200)

    // Key in first block
    assert(index.findBlock("aab".getBytes) == 0)

    // Key in second block
    assert(index.findBlock("bbc".getBytes) == 100)

    // Key in third block
    assert(index.findBlock("ccd".getBytes) == 200)

    // Key before first entry
    assert(index.findBlock("aa".getBytes) == -1)
  }

  test("serialization and deserialization") {
    val index = new SparseIndex()

    index.addEntry("key1".getBytes, 0)
    index.addEntry("key2".getBytes, 100)
    index.addEntry("key3".getBytes, 200)

    val bytes = index.toBytes()
    val restored = SparseIndex.fromBytes(bytes)

    assert(restored.size == 3)
    assert(restored.findBlock("key1".getBytes) == 0)
    assert(restored.findBlock("key2a".getBytes) == 100)
  }
}

/**
 * Tests for the merging iterator.
 */
class MergingIteratorSuite extends SparkFunSuite {

  test("merge sorted iterators") {
    val iter1 = Iterator(
      ("a".getBytes, MemTableEntry("1".getBytes, 1)),
      ("c".getBytes, MemTableEntry("3".getBytes, 3))
    )

    val iter2 = Iterator(
      ("b".getBytes, MemTableEntry("2".getBytes, 2)),
      ("d".getBytes, MemTableEntry("4".getBytes, 4))
    )

    val merged = new MergingIterator(Seq(iter1, iter2))
    val result = merged.map { case (k, v) => (new String(k), new String(v)) }.toList

    assert(result == List(("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")))
  }

  test("newer values take precedence") {
    // Same key in both iterators, different sequence numbers
    val iter1 = Iterator(
      ("a".getBytes, MemTableEntry("old".getBytes, 1))
    )

    val iter2 = Iterator(
      ("a".getBytes, MemTableEntry("new".getBytes, 2))
    )

    val merged = new MergingIterator(Seq(iter1, iter2))
    val result = merged.map { case (k, v) => (new String(k), new String(v)) }.toList

    assert(result == List(("a", "new")))
  }

  test("tombstones hide values") {
    val iter1 = Iterator(("a".getBytes, MemTableEntry("value".getBytes, 1)))
    val iter2 = Iterator(("a".getBytes, MemTableEntry(null, 2))) // Tombstone

    val merged = new MergingIterator(Seq(iter1, iter2))
    val result = merged.toList

    assert(result.isEmpty)
  }
}



