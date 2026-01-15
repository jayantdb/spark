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

import java.io._
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.util.UUID
import javax.annotation.concurrent.ThreadSafe

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging

/**
 * SSTable (Sorted String Table) - An immutable, sorted, disk-based key-value table.
 * An SSTable is a fundamental building block of the LSM-Tree architecture.
 * Once written, an SSTable is never modified - all updates create new SSTables. This immutability
 * enables safe concurrent reads without locks and simplifies crash recovery.
 *
 * FILE STRUCTURE:
 * The SSTable file is organized into four sequential sections:
 *   - Data Blocks: The bulk of the file containing sorted key-value pairs. Each block
 *     holds multiple entries up to a configurable size limit. Keys within each block
 *     are sorted, and blocks themselves are sorted relative to each other.
 *
 *   - Bloom Filter: A probabilistic data structure that quickly answers "might this key
 *     exist?" questions. If the Bloom filter says "no", the key is definitely not in
 *     this SSTable, avoiding unnecessary disk reads. False positives are possible but
 *     rare (configurable, typically 1%).
 *
 *   - Sparse Index: A lookup table mapping sampled keys to their block offsets. Instead
 *     of indexing every key, we record one key per N entries (the sparse index interval).
 *     Binary search on this index quickly locates the correct block.
 *
 *   - Footer: Fixed-size metadata including magic number, version, data size, Bloom
 *     filter size, sparse index size, entry count, and min/max keys.
 *
 * DATA BLOCK ENTRY FORMAT:
 * Data Block format:
 * {{{
 *    | Num Entries (4 bytes) |
 *    | Entry 1: key_len (4) | key | value_len (4) | value | deleted (1) | seq (8) |
 *    | Entry 2: ... |
 *    | CRC32 (4 bytes) |
 * }}}
 * Each entry within a data block contains:
 *   - key_length (4 bytes): Length of the key in bytes
 *   - key (variable): The actual key bytes
 *   - value_length (4 bytes): Length of the value, or -1 for tombstones (deleted keys)
 *   - value (variable): The actual value bytes (absent for tombstones)
 *   - deleted (1 byte): 1 if this entry is a tombstone, 0 otherwise
 *   - sequence_number (8 bytes): Monotonically increasing number for ordering updates
 *
 * @param file        Path to the SSTable file on disk
 * @param dataSize    Size of the data blocks section in bytes (used for memory mapping)
 * @param bloomFilter The Bloom filter for fast negative lookups
 * @param sparseIndex The sparse index for block-level binary search
 * @param minKey      The smallest key in this SSTable (for range filtering)
 * @param maxKey      The largest key in this SSTable (for range filtering)
 * @param numEntries  Total count of key-value entries in this SSTable
 */
@ThreadSafe
class SSTable private[state](
                              val file: File,
                              val dataSize: Long,
                              private val bloomFilter: LSMBloomFilter,
                              private[state] val sparseIndex: SparseIndex,
                              val minKey: Array[Byte],
                              val maxKey: Array[Byte],
                              val numEntries: Long
                            ) extends Closeable with Logging {

  import LSMTree.compareByteArrays

  // Memory-mapped buffer for fast random reads. Memory mapping allows the OS to manage
  // which portions of the file are loaded into RAM, avoiding explicit read() calls and
  // enabling the OS page cache to optimize access patterns.
  @volatile private var mappedBuffer: MappedByteBuffer = _
  @volatile private var fileChannel: FileChannel = _

  // Lazily initializes the memory mapping on first access. This avoids mapping files
  // that are never actually read (e.g., filtered out by Bloom filter or key range).
  // Thread-safe via double-checked locking.
  private def ensureMapped(): MappedByteBuffer = {
    if (mappedBuffer == null) {
      synchronized {
        if (mappedBuffer == null) {
          fileChannel = FileChannel.open(file.toPath, StandardOpenOption.READ)
          mappedBuffer = fileChannel.map(
            FileChannel.MapMode.READ_ONLY,
            0,
            dataSize
          )
        }
      }
    }
    mappedBuffer
  }

  /**
   * Checks if this SSTable might contain the given key using the Bloom filter.
   *
   * This is the first step in any key lookup. The Bloom filter provides a fast answer:
   * - returns false, the key is definetely not in this SSTable
   * - returns true, the key might be in this SSTable
   *
   * This helps reducing unnecessary disk reads when searching across multiple SSTables.
   */
  def mightContain(key: Array[Byte]): Boolean = {
    bloomFilter.mightContain(key)
  }

  /**
   * Retrieves the value associated with a key from this SSTable.
   *
   * The lookup process works in three steps:
   *   1. Use the sparse index to find which block might contain the key. The sparse index performs
   *      a binary search to locate the largest indexed key that is less than or equal to our
   *      search key, giving us the block's file offset.
   *      2. Read the block from disk (via memory mapping).
   *      3. Search within the block for the exact key. Small blocks use linear scan (better
   *      cache locality), while larger blocks use binary search (fewer comparisons).
   *
   * @param key The key to look up
   * @return The MemTableEntry containing the value and metadata, or null if not found
   */
  def get(key: Array[Byte]): MemTableEntry = {
    // Step 1: Use sparse index to find the block that might contain the key
    val blockOffset = sparseIndex.findBlock(key)
    if (blockOffset < 0) return null

    // Step 2: Memory-map the file and position at the block start
    val buffer = ensureMapped()
    buffer.position(blockOffset.toInt)

    val numBlockEntries = buffer.getInt()

    // Step 3: Search within the block. For blocks with 8 or fewer entries, linear scan
    // is faster due to sequential memory access patterns (cache-friendly). For larger
    // blocks, binary search reduces the number of key comparisons.
    if (numBlockEntries <= 8) return linearScanBlock(buffer, key, numBlockEntries)

    binarySearchBlock(buffer, blockOffset.toInt + 4, key, numBlockEntries)
  }

  /**
   * Performs a linear scan through a block to find a key.
   *
   * Linear scan reads entries sequentially from the buffer. This is optimal for small
   * blocks because: (a) fewer comparisons are needed anyway, and (b) sequential memory
   * access has excellent cache locality, making each read faster than random access.
   *
   * The scan stops early if we encounter a key greater than our search key (since
   * entries are sorted), avoiding unnecessary reads.
   */
  private def linearScanBlock(
                               buffer: MappedByteBuffer,
                               key: Array[Byte],
                               numEntries: Int
                             ): MemTableEntry = {
    for (_ <- 0 until numEntries) {
      val keyLen = buffer.getInt()
      val entryKey = new Array[Byte](keyLen)
      buffer.get(entryKey)

      val valueLen = buffer.getInt()
      val entryValue = if (valueLen >= 0) {
        val v = new Array[Byte](valueLen)
        buffer.get(v)
        v
      } else {
        null
      }

      val isDeleted = buffer.get() != 0
      val seqNum = buffer.getLong()

      val cmp = compareByteArrays(entryKey, key)
      if (cmp == 0) return MemTableEntry(if (isDeleted) null else entryValue, seqNum)
      else if (cmp > 0) return null
    }
    null
  }

  /**
   * Performs binary search within a block to find a key.
   *
   * For blocks with more than 8 entries, binary search reduces the number of key
   * comparisons from O(n) to O(log n). However, binary search requires random access
   * to entries, so we first build an offset array that maps each entry index to its
   * byte position within the block.
   *
   * The trade-off: we spend O(n) time building the offset array, but then only O(log n)
   * key comparisons. For large blocks with expensive key comparisons, this is worthwhile.
   */
  private def binarySearchBlock(
                                 buffer: MappedByteBuffer,
                                 blockStart: Int,
                                 key: Array[Byte],
                                 numEntries: Int
                               ): MemTableEntry = {
    // First, build offset array for binary search
    val offsets = new Array[Int](numEntries)
    var pos = blockStart

    for (i <- 0 until numEntries) {
      offsets(i) = pos
      val keyLen = buffer.getInt(pos)
      pos += 4 + keyLen
      val valueLen = buffer.getInt(pos)
      pos += 4 + (if (valueLen >= 0) valueLen else 0) + 1 + 8
    }

    // Binary search
    var low = 0
    var high = numEntries - 1

    while (low <= high) {
      val mid = (low + high) >>> 1
      buffer.position(offsets(mid))

      val keyLen = buffer.getInt()
      val entryKey = new Array[Byte](keyLen)
      buffer.get(entryKey)

      val cmp = compareByteArrays(entryKey, key)
      if (cmp < 0) {
        low = mid + 1
      } else if (cmp > 0) {
        high = mid - 1
      } else {
        // Found it! Read the value
        val valueLen = buffer.getInt()
        val entryValue = if (valueLen >= 0) {
          val v = new Array[Byte](valueLen)
          buffer.get(v)
          v
        } else {
          null
        }
        val isDeleted = buffer.get() != 0
        val seqNum = buffer.getLong()
        return MemTableEntry(if (isDeleted) null else entryValue, seqNum)
      }
    }

    null
  }

  /**
   * Returns an iterator over all entries in this SSTable in sorted key order.
   */
  def iterator: Iterator[(Array[Byte], MemTableEntry)] = { new SSTableIterator(this) }

  /**
   * Returns an iterator starting from a specific key (inclusive).
   *
   * This uses the sparse index to efficiently seek to the approximate position,
   * avoiding a full scan
   */
  def iteratorFrom(fromKey: Array[Byte]): Iterator[(Array[Byte], MemTableEntry)] = {
    new SSTableSeekIterator(this, fromKey, null)
  }

  /**
   * Returns an iterator for all keys matching a given prefix.
   */
  def prefixIterator(prefix: Array[Byte]): Iterator[(Array[Byte], MemTableEntry)] = {
    new SSTableSeekIterator(this, prefix, prefix)
  }

  /**
   * Returns the total file size of this SSTable in bytes.
   * Includes data blocks, Bloom filter, sparse index, and footer.
   */
  def size: Long = { file.length() }

  /**
   * Returns the estimated heap memory used by the Bloom filter.
   * The Bloom filter is kept in memory for fast negative lookups, so this
   * contributes to the overall memory footprint of the state store.
   */
  def bloomFilterMemory: Long = { bloomFilter.estimatedMemory }

  /**
   * Releases resources associated with this SSTable.
   *
   * Memory-mapped files in Java require special handling because the JVM does not
   * automatically unmap them when the MappedByteBuffer is garbage collected.
   * On Windows, this can prevent file deletion. Using reflection to access the internal Cleaner
   * and explicitly release the mapping.
   */
  override def close(): Unit = {
    synchronized {
      if (mappedBuffer != null) {
        val cleanerMethod = mappedBuffer.getClass.getMethod("cleaner")
        cleanerMethod.setAccessible(true)
        val cleaner = cleanerMethod.invoke(mappedBuffer)
        if (cleaner != null) {
          val cleanMethod = cleaner.getClass.getMethod("clean")
          cleanMethod.setAccessible(true)
          cleanMethod.invoke(cleaner)
        }
        mappedBuffer = null
      }

      if (fileChannel != null) {
        fileChannel.close()
        fileChannel = null
      }
    }
  }

  /**
   * Deletes the SSTable file from disk.
   * Closes any open resources first to ensure the file can be deleted (especially on Windows).
   */
  def delete(): Unit = {
    close()
    file.delete()
  }
}

object SSTable extends Logging {

  // Magic number 0x5354424C identifies valid SSTable files and guards against accidental corruption
  // or reading wrong file types.
  private val MAGIC_NUMBER = 0x5354424C // "STBL"

  // Version number for SSTable format. Increment when making incompatible changes to ensure old
  // versions can detect and reject newer format files.
  private val VERSION = 1

  /**
   * Creates an SSTable from a MemTable's contents.
   * This is called during the flush operation when a MemTable reaches its size limit.
   * All entries from the MemTable are written to a new SSTable file on disk.
   */
  def createFromMemTable(
                          memTable: MemTable,
                          directory: File,
                          blockSize: Int,
                          bloomFilterFpp: Double,
                          sparseIndexInterval: Int
                        ): SSTable = {

    val entries = memTable.iterator.toArray
    createFromEntriesInternal(
      entries.iterator.map { case (k, e) => (k, e) },
      entries.length,
      directory,
      blockSize,
      bloomFilterFpp,
      sparseIndexInterval
    )
  }

  /**
   * Creates an SSTable from a sequence of sorted entries.
   *
   * This is called during compaction when merging multiple SSTables. The entries must already be
   * sorted by key. Duplicate keys is being resolved before calling this method by keeping
   * only the entry with the highest sequence number).
   */
  def createFromEntries(
                         entries: Iterable[(Array[Byte], MemTableEntry)],
                         directory: File,
                         blockSize: Int,
                         bloomFilterFpp: Double,
                         sparseIndexInterval: Int
                       ): SSTable = {

    val entriesArray = entries.toArray
    createFromEntriesInternal(
      entriesArray.iterator,
      entriesArray.length,
      directory,
      blockSize,
      bloomFilterFpp,
      sparseIndexInterval
    )
  }

  /**
   * Internal method that performs the actual SSTable creation.
   *
   * This method is designed for memory efficiency. Instead of buffering all entries in memory
   * before writing, it streams data directly to disk. Memory usage remains
   * O(bloom_filter + sparse_index) regardless of how much data is being written.
   *
   * KEY COMPONENTS:
   *   - Bloom Filter: Built incrementally by adding each key as we stream. Uses
   *     approximately 1.2 bytes per key at 1% false positive probability.
   *   - Sparse Index: Records one key-to-offset mapping per sparseIndexInterval entries.
   *     Memory usage is about 24 bytes per entry (key clone + offset).
   *   - Data Blocks: Written directly to disk. Block boundaries are determined by the
   *     blockSize parameter. Each block contains multiple sorted entries.
   *
   * WRITE PERFORMANCE:
   * Using a 256 KB BufferedOutputStream for optimal sequential write performance.
   * CountingOutputStream wrapper tracks the current file position without requiring
   * additional buffering.
   */
  private def createFromEntriesInternal(
                                         entries: Iterator[(Array[Byte], MemTableEntry)],
                                         numEntries: Int,
                                         directory: File,
                                         blockSize: Int,
                                         bloomFilterFpp: Double,
                                         sparseIndexInterval: Int
                                       ): SSTable = {

    if (numEntries == 0) throw new IllegalArgumentException("Cannot create empty SSTable")

    val file = new File(directory, s"sstable_${UUID.randomUUID()}.sst")
    // Use 256KB buffer for optimal sequential write performance
    val fileOutputStream = new FileOutputStream(file)
    val bufferedOutput = new BufferedOutputStream(fileOutputStream, 256 * 1024)
    // CountingOutputStream tracks bytes written without buffering
    val countingOutput = new CountingOutputStream(bufferedOutput)
    val dataOutput = new DataOutputStream(countingOutput)

    try {
      val bloomFilter = LSMBloomFilter(numEntries, bloomFilterFpp)
      val sparseIndex = new SparseIndex()
      var currentBlockSize = 0
      var currentBlockEntries = 0
      var currentBlockStart = 0L
      var firstKeyInBlock: Array[Byte] = null
      var minKey: Array[Byte] = null
      var maxKey: Array[Byte] = null
      var entryCount = 0L
      var indexCounter = 0

      for ((key, entry) <- entries) {
        // Update min/max keys (only clone first and last)
        if (minKey == null) minKey = key.clone()
        maxKey = key // Will clone at the end

        // Add to Bloom filter (O(k) hash operations, k = number of hash functions)
        bloomFilter.put(key)

        // Calculate entry size for block management
        // 4: The bytes size for the key.
        // key.length: The bytes for the actual key data itself.
        // 4: The bytes size for the value
        // The actual value data: Takes entry.value.length bytes if a value exists, 0 if tombstone.
        // 1: A deleted flag. 1 means the entry is a tombstone, 0 means it's a normal entry.
        // 8: The bytes to store the sequence number as a Long. This is a monotonically inc number
        val entrySize = 4 + key.length + 4 +
          (if (entry.value != null) entry.value.length else 0) + 1 + 8

        // Start new block if current is too large
        if (currentBlockSize + entrySize > blockSize && currentBlockEntries > 0) {
          // Record block boundary in sparse index
          sparseIndex.addEntry(firstKeyInBlock, currentBlockStart)
          currentBlockStart = countingOutput.getBytesWritten
          currentBlockSize = 0
          currentBlockEntries = 0
          firstKeyInBlock = null
        }

        // First entry in block - record for sparse index
        if (firstKeyInBlock == null) firstKeyInBlock = key.clone()

        dataOutput.writeInt(key.length)
        dataOutput.write(key)

        if (entry.value != null) {
          dataOutput.writeInt(entry.value.length)
          dataOutput.write(entry.value)
        } else {
          dataOutput.writeInt(-1) // Tombstone marker
        }

        dataOutput.writeByte(if (entry.isDeleted) 1 else 0)
        dataOutput.writeLong(entry.sequenceNumber)

        currentBlockSize += entrySize
        currentBlockEntries += 1
        entryCount += 1

        // Add to sparse index at intervals
        indexCounter += 1
        if (indexCounter >= sparseIndexInterval) {
          sparseIndex.addEntry(key.clone(), countingOutput.getBytesWritten)
          indexCounter = 0
        }
      }

      // Finalize last block
      if (firstKeyInBlock != null) sparseIndex.addEntry(firstKeyInBlock, currentBlockStart)

      // Clone maxKey now (we only kept reference during iteration)
      maxKey = maxKey.clone()

      // Flush data section and record size
      dataOutput.flush()

      val dataSize = countingOutput.getBytesWritten

      // Write Bloom filter (serialized)
      val bloomBytes = bloomFilter.toBytes()
      bufferedOutput.write(bloomBytes)

      // Write sparse index (serialized)
      val indexBytes = sparseIndex.toBytes()
      bufferedOutput.write(indexBytes)

      // Write footer with metadata
      val footerOutput = new DataOutputStream(bufferedOutput)
      footerOutput.writeInt(minKey.length)
      footerOutput.write(minKey)
      footerOutput.writeInt(maxKey.length)
      footerOutput.write(maxKey)
      footerOutput.writeInt(MAGIC_NUMBER)
      footerOutput.writeInt(VERSION)
      footerOutput.writeLong(dataSize)
      footerOutput.writeInt(bloomBytes.length)
      footerOutput.writeInt(indexBytes.length)
      footerOutput.writeLong(entryCount)

      footerOutput.flush()
      bufferedOutput.flush()

      new SSTable(file, dataSize, bloomFilter, sparseIndex, minKey, maxKey, entryCount)
    } finally {
      bufferedOutput.close()
    }
  }

  /**
   * Loads an existing SSTable from a file on disk.
   *
   * This is called during startup to restore state from previously written SSTables.
   * The loading process is as follows:
   * 1. Read the file footer first (which contains data size, Bloom filter size,
   * index size, entry count, min/max keys), then loads the Bloom filter and sparse index
   * into memory.
   * 2. Verify the magic number and version to ensure file integrity.
   * 3. Load the Bloom filter into memory (required for fast lookups)
   * 4. Load the sparse index into memory (required for block-level seeks)
   * 5. Return an SSTable object.
   */
  def load(file: File): SSTable = {
    val raf = new RandomAccessFile(file, "r")
    try {
      val footerSize = 4 + 4 + 8 + 4 + 4 + 8 // 32 bytes. Last 6 pieces of footer.
      raf.seek(file.length() - footerSize) // This will position us to Magic number.

      val magic = raf.readInt()
      if (magic != MAGIC_NUMBER) throw new IOException(s"Invalid SSTable magic number: $magic")

      val version = raf.readInt()
      if (version != VERSION) throw new IOException(s"Unsupported SSTable version: $version")

      val dataSize = raf.readLong()
      val bloomSize = raf.readInt()
      val indexSize = raf.readInt()
      val numEntries = raf.readLong()
      val minKeyLen = raf.readInt()
      val minKey = new Array[Byte](minKeyLen)
      raf.readFully(minKey)
      val maxKeyLen = raf.readInt()
      val maxKey = new Array[Byte](maxKeyLen)
      raf.readFully(maxKey)
      raf.seek(dataSize)
      val bloomBytes = new Array[Byte](bloomSize)
      raf.readFully(bloomBytes)
      val bloomFilter = LSMBloomFilter.fromBytes(bloomBytes)
      val indexBytes = new Array[Byte](indexSize)
      raf.readFully(indexBytes)
      val sparseIndex = SparseIndex.fromBytes(indexBytes)

      new SSTable(file, dataSize, bloomFilter, sparseIndex, minKey, maxKey, numEntries)
    } finally {
      raf.close()
    }
  }
}

/**
 * Sequential iterator over all entries in an SSTable.
 *
 * This iterator reads entries in sorted key order, starting from the beginning of the
 * data section. It uses a memory-mapped buffer for efficient sequential reads, which
 * allows the OS to optimize prefetching and caching.
 *
 * IMPLEMENTATION NOTES:
 *
 *   - Creates its own memory mapping independent of the SSTable's mapping. This avoids
 *     contention and allows the iterator to maintain its own read position.
 *
 *   - Uses prefetching: the next entry is always read ahead so hasNext() can return
 *     immediately without I/O.
 *
 *   - Gracefully handles read errors by terminating iteration rather than throwing.
 *
 * @param ssTable The SSTable to iterate over
 */
private class SSTableIterator(ssTable: SSTable) extends Iterator[(Array[Byte], MemTableEntry)] {

  // Create our own memory mapping for independent position tracking.
  // The channel is closed immediately after mapping - the mapping remains valid.
  private val buffer = ssTable.synchronized {
    val channel = FileChannel.open(ssTable.file.toPath, StandardOpenOption.READ)
    try {
      channel.map(FileChannel.MapMode.READ_ONLY, 0, ssTable.dataSize)
    } finally {
      channel.close()
    }
  }

  private var entriesRemaining = ssTable.numEntries
  private var nextEntry: (Array[Byte], MemTableEntry) = _

  // Prefetch the first entry so hasNext() works correctly from the start
  fetchNext()

  private def fetchNext(): Unit = {
    if (entriesRemaining <= 0 || !buffer.hasRemaining) {
      nextEntry = null
      return
    }

    try {
      val keyLen = buffer.getInt()
      val key = new Array[Byte](keyLen)
      buffer.get(key)

      val valueLen = buffer.getInt()
      val value = if (valueLen >= 0) {
        val v = new Array[Byte](valueLen)
        buffer.get(v)
        v
      } else {
        null
      }

      val isDeleted = buffer.get() != 0
      val seqNum = buffer.getLong()

      nextEntry = (key, MemTableEntry(if (isDeleted) null else value, seqNum))
      entriesRemaining -= 1
    } catch {
      case _: Exception =>
        nextEntry = null
        entriesRemaining = 0
    }
  }

  override def hasNext: Boolean = nextEntry != null

  override def next(): (Array[Byte], MemTableEntry) = {
    if (nextEntry == null) {
      throw new NoSuchElementException("No more entries")
    }
    val result = nextEntry
    fetchNext()
    result
  }
}

/**
 * Seeking iterator that can start from a specific key and optionally filter by prefix.
 *
 * Unlike the sequential SSTableIterator, this iterator uses the sparse index to jump
 * directly to the approximate location of the starting key, avoiding a full scan from
 * the beginning of the file.
 *
 * HOW SEEKING WORKS:
 *
 *   1. Use the sparse index to find the block that might contain the seek key. The
 *      sparse index gives us the largest indexed key <= our seek key, and its block offset.
 *
 * 2. Position the buffer at that block offset and begin scanning forward.
 *
 * 3. Skip entries until we find one with key >= seekKey. This is the starting point.
 *
 * PREFIX FILTERING:
 *
 * When a prefix filter is provided, the iterator automatically stops when it encounters
 * a key that no longer matches the prefix. This is efficient because keys are sorted -
 * once we pass the prefix boundary, no more matching keys can exist.
 *
 * @param ssTable      The SSTable to iterate over
 * @param seekKey      The key to start iteration from (inclusive lower bound)
 * @param prefixFilter Optional prefix filter - iteration stops when keys no longer match
 */
private class SSTableSeekIterator(
                                   ssTable: SSTable,
                                   seekKey: Array[Byte],
                                   prefixFilter: Array[Byte]
                                 ) extends Iterator[(Array[Byte], MemTableEntry)] {

  import LSMTree.compareByteArrays

  private val buffer = ssTable.synchronized {
    val channel = FileChannel.open(ssTable.file.toPath, StandardOpenOption.READ)
    try {
      channel.map(FileChannel.MapMode.READ_ONLY, 0, ssTable.dataSize)
    } finally {
      channel.close()
    }
  }

  private var nextEntry: (Array[Byte], MemTableEntry) = _
  private var exhausted = false

  // Seek to the starting position using sparse index
  seekToStart()
  // Then find the exact key
  findStartKey()

  private def seekToStart(): Unit = {
    val blockOffset = ssTable.sparseIndex.findBlock(seekKey)
    if (blockOffset >= 0) {
      buffer.position(blockOffset.toInt)
      // Skip block entry count
      if (buffer.hasRemaining) {
        buffer.getInt()
      }
    }
    // If blockOffset < 0, start from beginning (already at position 0)
  }

  private def findStartKey(): Unit = {
    // Scan forward until we find a key >= seekKey
    while (!exhausted && nextEntry == null) {
      if (!buffer.hasRemaining) {
        exhausted = true
        return
      }

      try {
        val keyLen = buffer.getInt()
        val key = new Array[Byte](keyLen)
        buffer.get(key)

        val valueLen = buffer.getInt()
        val value = if (valueLen >= 0) {
          val v = new Array[Byte](valueLen)
          buffer.get(v)
          v
        } else {
          null
        }

        val isDeleted = buffer.get() != 0
        val seqNum = buffer.getLong()

        // Check if key >= seekKey
        if (compareByteArrays(key, seekKey) >= 0) {
          // Check prefix filter
          if (prefixFilter == null || hasPrefix(key, prefixFilter)) {
            nextEntry = (key, MemTableEntry(if (isDeleted) null else value, seqNum))
          } else {
            // Key doesn't match prefix, we're done
            exhausted = true
          }
        }
        // If key < seekKey, continue scanning
      } catch {
        case _: Exception =>
          exhausted = true
      }
    }
  }

  private def fetchNext(): Unit = {
    nextEntry = null

    if (exhausted || !buffer.hasRemaining) {
      exhausted = true
      return
    }

    try {
      val keyLen = buffer.getInt()
      val key = new Array[Byte](keyLen)
      buffer.get(key)

      val valueLen = buffer.getInt()
      val value = if (valueLen >= 0) {
        val v = new Array[Byte](valueLen)
        buffer.get(v)
        v
      } else {
        null
      }

      val isDeleted = buffer.get() != 0
      val seqNum = buffer.getLong()

      // Check prefix filter if set
      if (prefixFilter != null && !hasPrefix(key, prefixFilter)) {
        exhausted = true
        return
      }

      nextEntry = (key, MemTableEntry(if (isDeleted) null else value, seqNum))
    } catch {
      case _: Exception =>
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
    if (nextEntry == null) {
      throw new NoSuchElementException("No more entries")
    }
    val result = nextEntry
    fetchNext()
    result
  }
}

/**
 * Bloom filter for fast probabilistic key membership testing.
 *
 * A Bloom filter is a space-efficient probabilistic data structure that answers the
 * question "is this key in the set?" with either "definitely not" or "maybe yes".
 * There are no false negatives - if the filter says "no", the key is definitely absent.
 * False positives are possible but controlled by the false positive probability (FPP).
 *
 * ALGORITHM (Kirsch-Mitzenmacher Optimization):
 *
 * Instead of computing k independent hash functions, we use a technique from the 2006
 * paper by Kirsch and Mitzenmacher that derives k hash values from just two base hashes.
 * The formula is: h_i(x) = h1(x) + i * h2(x)
 *
 * This is mathematically proven to maintain the same false positive probability while
 * being much faster (only 2 hash computations instead of k).
 *
 * WHY BLOOM FILTERS IN LSM-TREES:
 *
 * LSM-Trees may have many SSTables, and a key lookup might need to search all of them.
 * Bloom filters let us skip SSTables that definitely don't contain the key. With 1% FPP,
 * we eliminate 99% of unnecessary disk reads on average.
 *
 * MEMORY USAGE:
 *
 * At 1% FPP, a Bloom filter uses approximately 9.6 bits (1.2 bytes) per key. For example,
 * 1 million keys requires about 1.2 MB. This is much smaller than storing actual keys.
 */
class LSMBloomFilter private[state](
                                     private val numBits: Int,
                                     private val numHashFunctions: Int,
                                     private val bitArray: Array[Long]) {

  /**
   * Adds a key to the Bloom filter.
   *
   * Sets k bits in the bit array based on the key's hash values. Once a bit is set,
   * it stays set (Bloom filters don't support deletion).
   */
  def put(key: Array[Byte]): Unit = {
    val (h1, h2) = hash(key)
    for (i <- 0 until numHashFunctions) {
      // Kirsch-Mitzenmacher: h_i = h1 + i * h2
      val bit = ((h1 + i.toLong * h2) & Long.MaxValue) % numBits
      val longIndex = (bit / 64).toInt
      val bitIndex = (bit % 64).toInt
      bitArray(longIndex) |= (1L << bitIndex)
    }
  }

  /**
   * Checks if a key might be in the set.
   *
   * Returns false if the key is DEFINITELY NOT in the set (100% accurate).
   * Returns true if the key MIGHT be in the set (small chance of false positive).
   *
   * The check verifies that all k bits for this key are set. If any bit is 0,
   * the key was never added.
   */
  def mightContain(key: Array[Byte]): Boolean = {
    val (h1, h2) = hash(key)
    for (i <- 0 until numHashFunctions) {
      val bit = ((h1 + i.toLong * h2) & Long.MaxValue) % numBits
      val longIndex = (bit / 64).toInt
      val bitIndex = (bit % 64).toInt
      if ((bitArray(longIndex) & (1L << bitIndex)) == 0) {
        return false // Definitely not in the set
      }
    }
    true // Might be in the set
  }

  /**
   * Serializes the Bloom filter to a byte array for storage.
   * Format: numBits (4 bytes) + numHashFunctions (4 bytes) + bit array (variable)
   */
  def toBytes(): Array[Byte] = {
    val buffer = ByteBuffer.allocate(4 + 4 + bitArray.length * 8)
    buffer.putInt(numBits)
    buffer.putInt(numHashFunctions)
    for (l <- bitArray) {
      buffer.putLong(l)
    }
    buffer.array()
  }

  /**
   * Returns the estimated heap memory usage of this Bloom filter in bytes.
   * Includes the bit array plus object overhead.
   */
  def estimatedMemory: Long = bitArray.length * 8L + 16

  /**
   * Computes two independent 64-bit MurmurHash3 values for a key.
   * These serve as the base hashes h1 and h2 for the Kirsch-Mitzenmacher optimization.
   * Different seed values ensure the hashes are independent.
   */
  private def hash(key: Array[Byte]): (Long, Long) = {
    val h1 = MurmurHash3.hash64(key, 0, key.length, 0x9747b28c)
    val h2 = MurmurHash3.hash64(key, 0, key.length, 0xe17a1465)
    (h1, h2)
  }
}

/**
 * Factory object for creating and deserializing Bloom filters.
 */
object LSMBloomFilter {

  /**
   * Creates a new Bloom filter optimized for the expected number of keys.
   *
   * The size of the bit array and number of hash functions are calculated using
   * well-known optimal formulas that minimize the false positive probability for
   * a given memory budget.
   *
   * OPTIMAL SIZE FORMULA:
   *
   * The optimal number of bits is: m = -n * ln(p) / (ln(2))^2
   *
   * Where:
   *   - n = expected number of insertions
   *   - p = desired false positive probability (e.g., 0.01 for 1%)
   *
   * For p = 0.01 (1% FPP), this works out to about 9.6 bits per key.
   *
   * OPTIMAL HASH FUNCTION COUNT:
   *
   * The optimal number of hash functions is: k = (m/n) * ln(2)
   *
   * For p = 0.01, this is approximately 7 hash functions.
   *
   * @param expectedInsertions The number of keys expected to be inserted
   * @param fpp                The desired false positive probability (0.01 = 1%)
   */
  def apply(expectedInsertions: Int, fpp: Double): LSMBloomFilter = {
    // Calculate optimal number of bits using the formula: m = -n * ln(p) / (ln(2))^2
    // Ensure at least 64 bits (one Long) to avoid edge cases
    val numBits = math.ceil(-expectedInsertions * math.log(fpp) /
      (math.log(2) * math.log(2))).toInt.max(64)

    // Calculate optimal number of hash functions: k = (m/n) * ln(2)
    // Ensure at least 1 hash function
    val numHashFunctions = math.ceil(numBits.toDouble /
      expectedInsertions * math.log(2)).toInt.max(1)

    // Allocate bit array as Array[Long] for efficient 64-bit operations
    val numLongs = (numBits + 63) / 64
    val bitArray = new Array[Long](numLongs)

    new LSMBloomFilter(numBits, numHashFunctions, bitArray)
  }

  /**
   * Reconstructs a Bloom filter from its serialized byte representation.
   * Used when loading SSTables from disk.
   */
  def fromBytes(bytes: Array[Byte]): LSMBloomFilter = {
    val buffer = ByteBuffer.wrap(bytes)
    val numBits = buffer.getInt()
    val numHashFunctions = buffer.getInt()
    val numLongs = (numBits + 63) / 64
    val bitArray = new Array[Long](numLongs)
    for (i <- 0 until numLongs) {
      bitArray(i) = buffer.getLong()
    }
    new LSMBloomFilter(numBits, numHashFunctions, bitArray)
  }
}

/**
 * MurmurHash3 128-bit hash function (64-bit output variant).
 *
 * MurmurHash3 is a non-cryptographic hash function known for:
 *   - Excellent distribution: produces uniformly distributed hash values
 *   - High performance: optimized for speed with minimal collisions
 *   - Avalanche effect: small input changes cause large output changes
 *
 * This implementation is based on the x64 128-bit variant from the original
 * MurmurHash3 by Austin Appleby. We use different seeds to generate two
 * independent 64-bit hash values for the Bloom filter.
 *
 * Reference: https://github.com/aappleby/smhasher/wiki/MurmurHash3
 */
object MurmurHash3 {

  // Magic constants from the MurmurHash3 specification.
  // These specific values are chosen to provide good mixing properties.
  private val C1: Long = 0x87c37b91114253d5L
  private val C2: Long = 0x4cf5ad432745937fL

  /**
   * Computes a 64-bit MurmurHash3 hash of the input data.
   *
   * @param data   The byte array to hash
   * @param offset Starting offset in the array
   * @param length Number of bytes to hash
   * @param seed   Initial seed value (different seeds produce different hashes)
   * @return A 64-bit hash value
   */
  def hash64(data: Array[Byte], offset: Int, length: Int, seed: Int): Long = {
    var h = seed.toLong
    val nblocks = length / 8

    // Process the input in 8-byte blocks for efficiency
    for (i <- 0 until nblocks) {
      val k = getLongLittleEndian(data, offset + i * 8)
      h = mix64(h, k)
    }

    // Handle remaining bytes (0-7) that don't fill a complete block
    val tailOffset = offset + nblocks * 8
    var k: Long = 0
    length & 7 match {
      case 7 => k ^= (data(tailOffset + 6).toLong & 0xff) << 48
        k ^= (data(tailOffset + 5).toLong & 0xff) << 40
        k ^= (data(tailOffset + 4).toLong & 0xff) << 32
        k ^= (data(tailOffset + 3).toLong & 0xff) << 24
        k ^= (data(tailOffset + 2).toLong & 0xff) << 16
        k ^= (data(tailOffset + 1).toLong & 0xff) << 8
        k ^= data(tailOffset).toLong & 0xff
        h ^= mixK(k)
      case 6 => k ^= (data(tailOffset + 5).toLong & 0xff) << 40
        k ^= (data(tailOffset + 4).toLong & 0xff) << 32
        k ^= (data(tailOffset + 3).toLong & 0xff) << 24
        k ^= (data(tailOffset + 2).toLong & 0xff) << 16
        k ^= (data(tailOffset + 1).toLong & 0xff) << 8
        k ^= data(tailOffset).toLong & 0xff
        h ^= mixK(k)
      case 5 => k ^= (data(tailOffset + 4).toLong & 0xff) << 32
        k ^= (data(tailOffset + 3).toLong & 0xff) << 24
        k ^= (data(tailOffset + 2).toLong & 0xff) << 16
        k ^= (data(tailOffset + 1).toLong & 0xff) << 8
        k ^= data(tailOffset).toLong & 0xff
        h ^= mixK(k)
      case 4 => k ^= (data(tailOffset + 3).toLong & 0xff) << 24
        k ^= (data(tailOffset + 2).toLong & 0xff) << 16
        k ^= (data(tailOffset + 1).toLong & 0xff) << 8
        k ^= data(tailOffset).toLong & 0xff
        h ^= mixK(k)
      case 3 => k ^= (data(tailOffset + 2).toLong & 0xff) << 16
        k ^= (data(tailOffset + 1).toLong & 0xff) << 8
        k ^= data(tailOffset).toLong & 0xff
        h ^= mixK(k)
      case 2 => k ^= (data(tailOffset + 1).toLong & 0xff) << 8
        k ^= data(tailOffset).toLong & 0xff
        h ^= mixK(k)
      case 1 => k ^= data(tailOffset).toLong & 0xff
        h ^= mixK(k)
      case 0 => // Do nothing
    }

    // Finalization
    h ^= length
    fmix64(h)
  }

  private def mix64(h: Long, k: Long): Long = {
    var hh = h
    var kk = k
    kk *= C1
    kk = java.lang.Long.rotateLeft(kk, 31)
    kk *= C2
    hh ^= kk
    hh = java.lang.Long.rotateLeft(hh, 27)
    hh * 5 + 0x52dce729
  }

  private def mixK(k: Long): Long = {
    var kk = k
    kk *= C1
    kk = java.lang.Long.rotateLeft(kk, 31)
    kk *= C2
    kk
  }

  private def fmix64(k: Long): Long = {
    var kk = k
    kk ^= kk >>> 33
    kk *= 0xff51afd7ed558ccdL
    kk ^= kk >>> 33
    kk *= 0xc4ceb9fe1a85ec53L
    kk ^= kk >>> 33
    kk
  }

  private def getLongLittleEndian(data: Array[Byte], offset: Int): Long = {
    (data(offset).toLong & 0xff) |
      ((data(offset + 1).toLong & 0xff) << 8) |
      ((data(offset + 2).toLong & 0xff) << 16) |
      ((data(offset + 3).toLong & 0xff) << 24) |
      ((data(offset + 4).toLong & 0xff) << 32) |
      ((data(offset + 5).toLong & 0xff) << 40) |
      ((data(offset + 6).toLong & 0xff) << 48) |
      ((data(offset + 7).toLong & 0xff) << 56)
  }
}

/**
 * Sparse index for fast block-level lookups in SSTables.
 *
 * WHY SPARSE INDEXING:
 *
 * A "dense" index would map every key to its file offset, but this requires too much
 * memory for large SSTables. Instead, we use a "sparse" index that only records one
 * key per N entries (controlled by sparseIndexInterval).
 *
 * During lookup, we binary search the sparse index to find the block containing our
 * key, then scan within that block. This trades a small amount of extra scanning for
 * significant memory savings.
 *
 * EXAMPLE:
 *
 * If sparseIndexInterval = 64 and we have 1000 keys, the sparse index has about 16
 * entries. Binary search finds the right block in O(log 16) = 4 comparisons, then
 * we scan up to 64 entries within the block.
 *
 * DATA STRUCTURE:
 *
 * Internally stores an array of (key, offset) pairs sorted by key. Each key is the
 * first key of a block, and the offset is the byte position of that block in the file.
 */
class SparseIndex {

  import LSMTree.compareByteArrays

  // Array of (key, offset) pairs. Keys are cloned to avoid mutation issues.
  private val entries = new ArrayBuffer[(Array[Byte], Long)]()

  /**
   * Adds a new entry to the sparse index.
   * The key is cloned to ensure immutability.
   */
  def addEntry(key: Array[Byte], offset: Long): Unit = {
    entries += ((key.clone(), offset))
  }

  /**
   * Finds the block that might contain the given key using binary search.
   *
   * The algorithm finds the largest indexed key that is less than or equal to the
   * search key. This gives us the block where the key would exist if it's in the
   * SSTable at all.
   *
   * @param key The key to search for
   * @return The byte offset of the block, or -1 if the key is smaller than all indexed keys
   */
  def findBlock(key: Array[Byte]): Long = {
    if (entries.isEmpty) return -1

    // Binary search for the largest entry with key <= search key
    var low = 0
    var high = entries.length - 1
    var result = -1L

    while (low <= high) {
      val mid = (low + high) / 2
      val cmp = compareByteArrays(entries(mid)._1, key)

      if (cmp <= 0) {
        // This entry's key <= search key, so this block might contain our key
        result = entries(mid)._2
        low = mid + 1 // Look for a larger matching entry
      } else {
        high = mid - 1
      }
    }

    result
  }

  /**
   * Returns the number of entries in the sparse index.
   * This indicates roughly how many blocks the SSTable has.
   */
  def size: Int = entries.length

  /**
   * Serializes the sparse index to a byte array for storage in the SSTable file.
   * Format: count (4 bytes) + [key_length (4 bytes) + key + offset (8 bytes)] for each entry
   */
  def toBytes(): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val out = new DataOutputStream(baos)

    out.writeInt(entries.length)
    for ((key, offset) <- entries) {
      out.writeInt(key.length)
      out.write(key)
      out.writeLong(offset)
    }

    out.flush()
    baos.toByteArray
  }
}

/**
 * Factory for deserializing sparse indexes from byte arrays.
 */
object SparseIndex {

  /**
   * Reconstructs a sparse index from its serialized byte representation.
   * Used when loading SSTables from disk.
   */
  def fromBytes(bytes: Array[Byte]): SparseIndex = {
    val input = new DataInputStream(new ByteArrayInputStream(bytes))
    val index = new SparseIndex()

    val numEntries = input.readInt()
    for (_ <- 0 until numEntries) {
      val keyLen = input.readInt()
      val key = new Array[Byte](keyLen)
      input.readFully(key)
      val offset = input.readLong()
      index.addEntry(key, offset)
    }

    index
  }
}

/**
 * A lightweight OutputStream wrapper that counts bytes written without buffering.
 *
 * PURPOSE:
 *
 * During SSTable creation, we need to know the current file position to record
 * block offsets in the sparse index. Without this wrapper, we would need to either:
 *   - Buffer everything in memory (defeats streaming write benefit)
 *   - Flush after each write and query file position (slow)
 *
 * CountingOutputStream solves this by simply incrementing a counter on each write.
 * The overhead is negligible (just an addition per write call).
 *
 * USAGE:
 *
 * Wrap the BufferedOutputStream with CountingOutputStream before wrapping with
 * DataOutputStream. This ensures all bytes are counted regardless of which write
 * method is used.
 *
 * @param out The underlying output stream to write to
 */
private[state] class CountingOutputStream(out: OutputStream) extends OutputStream {

  private var bytesWritten: Long = 0L

  /**
   * Returns the total number of bytes written through this stream.
   * This value represents the current position in the output file.
   */
  def getBytesWritten: Long = bytesWritten

  override def write(b: Int): Unit = {
    out.write(b)
    bytesWritten += 1
  }

  override def write(b: Array[Byte]): Unit = {
    out.write(b)
    bytesWritten += b.length
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    out.write(b, off, len)
    bytesWritten += len
  }

  override def flush(): Unit = out.flush()

  override def close(): Unit = out.close()
}
