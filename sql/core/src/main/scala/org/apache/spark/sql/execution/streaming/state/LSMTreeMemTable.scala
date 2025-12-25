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

import java.util.Comparator
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.concurrent.ThreadSafe

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging

/**
 * Entry stored in the MemTable.
 *
 * @param value The value bytes, or null if this is a tombstone (deletion marker)
 * @param sequenceNumber Monotonically increasing sequence number for ordering
 */
case class MemTableEntry(
    value: Array[Byte],
    sequenceNumber: Long) {

  def isDeleted: Boolean = value == null

  def estimatedSize: Int = {
    // 8 bytes for sequenceNumber + value size
    8 + (if (value != null) value.length else 0)
  }
}

/**
 * High-performance in-memory table for LSM-Tree.
 *
 * Uses ConcurrentSkipListMap for:
 * - O(log n) insertions, lookups, and deletions
 * - Lock-free concurrent reads
 * - Natural sorted order for efficient range scans and SSTable creation
 *
 * The MemTable uses off-heap memory where possible to reduce GC pressure:
 * - Keys and values are stored as byte arrays
 * - No object overhead per entry beyond the SkipList node
 *
 * @param maxSize Maximum size in bytes before the MemTable should be flushed
 */
@ThreadSafe
class MemTable(maxSize: Long) extends Logging {

  import MemTable._

  // Byte array comparator for sorted storage
  private val comparator: Comparator[Array[Byte]] = (a: Array[Byte], b: Array[Byte]) => {
    LSMTree.compareByteArrays(a, b)
  }

  // The underlying concurrent skip list
  // Using Array[Byte] as key for memory efficiency
  private val data = new ConcurrentSkipListMap[Array[Byte], MemTableEntry](comparator)

  // Estimated size tracking (atomic for concurrent updates)
  private val estimatedSizeBytes = new AtomicLong(0L)

  // Monotonically increasing sequence number
  private val sequenceCounter = new AtomicLong(0L)

  /**
   * Put a key-value pair into the MemTable.
   *
   * @param key The key bytes
   * @param value The value bytes
   */
  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    val entry = MemTableEntry(value, sequenceCounter.incrementAndGet())
    val oldEntry = data.put(key, entry)

    // Update size estimate
    val newSize = ENTRY_OVERHEAD + key.length + entry.estimatedSize
    val oldSize = if (oldEntry != null) {
      ENTRY_OVERHEAD + key.length + oldEntry.estimatedSize
    } else {
      0
    }
    estimatedSizeBytes.addAndGet(newSize - oldSize)
  }

  /**
   * Delete a key (insert a tombstone).
   *
   * @param key The key to delete
   */
  def delete(key: Array[Byte]): Unit = {
    val entry = MemTableEntry(null, sequenceCounter.incrementAndGet())
    val oldEntry = data.put(key, entry)

    // Update size estimate
    val newSize = ENTRY_OVERHEAD + key.length + entry.estimatedSize
    val oldSize = if (oldEntry != null) {
      ENTRY_OVERHEAD + key.length + oldEntry.estimatedSize
    } else {
      0
    }
    estimatedSizeBytes.addAndGet(newSize - oldSize)
  }

  /**
   * Get the entry for a key.
   *
   * @param key The key to look up
   * @return The entry, or null if not found
   */
  def get(key: Array[Byte]): MemTableEntry = {
    data.get(key)
  }

  /**
   * Check if a key exists in the MemTable.
   *
   * @param key The key to check
   * @return true if the key exists (including tombstones)
   */
  def contains(key: Array[Byte]): Boolean = {
    data.containsKey(key)
  }

  /**
   * Get an iterator over all entries in sorted order.
   */
  def iterator: Iterator[(Array[Byte], MemTableEntry)] = {
    data.entrySet().iterator().asScala.map(e => (e.getKey, e.getValue))
  }

  /**
   * Get an iterator starting from the given key (inclusive).
   * Uses ConcurrentSkipListMap.tailMap for O(log n) seek.
   */
  def iteratorFrom(fromKey: Array[Byte]): Iterator[(Array[Byte], MemTableEntry)] = {
    data.tailMap(fromKey, true).entrySet().iterator().asScala.map(e => (e.getKey, e.getValue))
  }

  /**
   * Get an iterator for keys in the range [fromKey, toKey).
   * Uses ConcurrentSkipListMap.subMap for O(log n) seek.
   */
  def iteratorRange(
      fromKey: Array[Byte],
      toKey: Array[Byte]): Iterator[(Array[Byte], MemTableEntry)] = {
    data.subMap(fromKey, true, toKey, false)
      .entrySet().iterator().asScala.map(e => (e.getKey, e.getValue))
  }

  /**
   * Get an iterator for keys with the given prefix.
   * Efficiently seeks to the prefix start and stops when prefix no longer matches.
   */
  def prefixIterator(prefix: Array[Byte]): Iterator[(Array[Byte], MemTableEntry)] = {
    // Seek to start of prefix
    val tailIterator = data.tailMap(prefix, true).entrySet().iterator().asScala

    // Take while prefix matches
    new Iterator[(Array[Byte], MemTableEntry)] {
      private var nextEntry: (Array[Byte], MemTableEntry) = _
      private var exhausted = false

      fetchNext()

      private def fetchNext(): Unit = {
        if (exhausted || !tailIterator.hasNext) {
          nextEntry = null
          exhausted = true
          return
        }

        val entry = tailIterator.next()
        val key = entry.getKey

        // Check if key still has the prefix
        if (hasPrefix(key, prefix)) {
          nextEntry = (key, entry.getValue)
        } else {
          // Prefix no longer matches, we're done
          nextEntry = null
          exhausted = true
        }
      }

      private def hasPrefix(key: Array[Byte], prefix: Array[Byte]): Boolean = {
        if (key.length < prefix.length) return false
        var i = 0
        while (i < prefix.length) {
          if (key(i) != prefix(i)) return false
          i += 1
        }
        true
      }

      override def hasNext: Boolean = nextEntry != null

      override def next(): (Array[Byte], MemTableEntry) = {
        if (nextEntry == null) throw new NoSuchElementException()
        val result = nextEntry
        fetchNext()
        result
      }
    }
  }

  /**
   * Get the number of entries (including tombstones).
   */
  def size: Int = data.size()

  /**
   * Get the estimated memory usage in bytes.
   */
  def estimatedSize: Long = estimatedSizeBytes.get()

  /**
   * Check if the MemTable is empty.
   */
  def isEmpty: Boolean = data.isEmpty

  /**
   * Clear all entries.
   */
  def clear(): Unit = {
    data.clear()
    estimatedSizeBytes.set(0L)
    sequenceCounter.set(0L)
  }

  /**
   * Get the first (smallest) key.
   */
  def firstKey: Array[Byte] = {
    if (data.isEmpty) null else data.firstKey()
  }

  /**
   * Get the last (largest) key.
   */
  def lastKey: Array[Byte] = {
    if (data.isEmpty) null else data.lastKey()
  }

}

object MemTable {
  // Estimated overhead per entry in the SkipList
  // Includes: SkipList node overhead, key/value array references
  private val ENTRY_OVERHEAD: Int = 64
}

/**
 * A merging iterator that combines multiple sorted iterators into a single
 * sorted iterator, with newer values taking precedence over older ones.
 *
 * This is used to merge MemTable and SSTable iterators during reads.
 */
class MergingIterator(
    iterators: Seq[Iterator[(Array[Byte], MemTableEntry)]])
    extends Iterator[(Array[Byte], Array[Byte])] {

  import LSMTree.compareByteArrays

  // Priority queue to track the current minimum key from each iterator
  private case class IteratorEntry(
      key: Array[Byte],
      entry: MemTableEntry,
      iteratorIndex: Int)

  private val ordering: Ordering[IteratorEntry] = (a: IteratorEntry, b: IteratorEntry) => {
    val keyCompare = compareByteArrays(b.key, a.key) // Reversed for min-heap
    if (keyCompare != 0) keyCompare
    else {
      // Same key: prefer higher sequence number (newer)
      java.lang.Long.compare(b.entry.sequenceNumber, a.entry.sequenceNumber)
    }
  }

  private val heap = new java.util.PriorityQueue[IteratorEntry](
    iterators.size.max(1),
    ordering)

  // Active iterator states
  private val activeIterators: Array[Iterator[(Array[Byte], MemTableEntry)]] =
    iterators.toArray

  // Initialize heap with first element from each iterator
  for ((iter, idx) <- activeIterators.zipWithIndex) {
    if (iter.hasNext) {
      val (key, entry) = iter.next()
      heap.add(IteratorEntry(key, entry, idx))
    }
  }

  // Next item to return (after deduplication)
  private var nextItem: (Array[Byte], Array[Byte]) = _
  private var lastReturnedKey: Array[Byte] = _

  // Prefetch the next item
  prefetchNext()

  private def prefetchNext(): Unit = {
    nextItem = null

    while (!heap.isEmpty && nextItem == null) {
      val minEntry = heap.poll()

      // Advance the iterator that provided this entry
      val iter = activeIterators(minEntry.iteratorIndex)
      if (iter.hasNext) {
        val (key, entry) = iter.next()
        heap.add(IteratorEntry(key, entry, minEntry.iteratorIndex))
      }

      // Skip duplicate keys (we already returned a newer version)
      if (lastReturnedKey == null ||
          compareByteArrays(minEntry.key, lastReturnedKey) != 0) {
        // Skip tombstones
        if (!minEntry.entry.isDeleted) {
          nextItem = (minEntry.key, minEntry.entry.value)
          lastReturnedKey = minEntry.key
        } else {
          // Mark key as seen even for tombstones to skip older versions
          lastReturnedKey = minEntry.key
        }
      }
      // else: skip this entry, it's an older version of a key we already returned
    }
  }

  override def hasNext: Boolean = nextItem != null

  override def next(): (Array[Byte], Array[Byte]) = {
    if (nextItem == null) {
      throw new NoSuchElementException("No more elements")
    }
    val result = nextItem
    prefetchNext()
    result
  }
}
