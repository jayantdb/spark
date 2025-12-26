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
 * SSTable (Sorted String Table) - An immutable, sorted, disk-based table.
 *
 * File format:
 * {{{
 *   +------------------+
 *   | Data Blocks      |   <- Variable size blocks of sorted key-value pairs
 *   +------------------+
 *   | Bloom Filter     |   <- Probabilistic filter for fast negative lookups
 *   +------------------+
 *   | Sparse Index     |   <- Index pointing to data blocks
 *   +------------------+
 *   | Footer           |   <- Metadata (offsets, sizes, etc.)
 *   +------------------+
 * }}}
 *
 * Data Block format:
 * {{{
 *   | Num Entries (4 bytes) |
 *   | Entry 1: key_len (4) | key | value_len (4) | value | deleted (1) | seq (8) |
 *   | Entry 2: ... |
 *   | CRC32 (4 bytes) |
 * }}}
 *
 * @param file Path to the SSTable file
 * @param dataSize Size of the data section
 * @param bloomFilter The Bloom filter for this SSTable
 * @param sparseIndex The sparse index for this SSTable
 * @param minKey Minimum key in this SSTable
 * @param maxKey Maximum key in this SSTable
 * @param numEntries Total number of entries
 */
@ThreadSafe
class SSTable private[state] (
    val file: File,
    val dataSize: Long,
    private val bloomFilter: LSMBloomFilter,
    private[state] val sparseIndex: SparseIndex,
    val minKey: Array[Byte],
    val maxKey: Array[Byte],
    val numEntries: Long) extends Closeable with Logging {

  import LSMTree.compareByteArrays

  // Memory-mapped file for fast reads
  @volatile private var mappedBuffer: MappedByteBuffer = _
  @volatile private var fileChannel: FileChannel = _

  // Lazy initialization of memory mapping
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
   * Check if this SSTable might contain the key (using Bloom filter).
   */
  def mightContain(key: Array[Byte]): Boolean = {
    bloomFilter.mightContain(key)
  }

  /**
   * Get the value for a key.
   *
   * Uses the sparse index to find the right block, then scans the block.
   */
  def get(key: Array[Byte]): MemTableEntry = {
    // Use sparse index to find the block that might contain the key
    val blockOffset = sparseIndex.findBlock(key)
    if (blockOffset < 0) {
      return null
    }

    // Read the block
    val buffer = ensureMapped()
    buffer.position(blockOffset.toInt)

    val numBlockEntries = buffer.getInt()

    // For very small blocks, linear scan is faster due to cache locality
    if (numBlockEntries <= 8) {
      return linearScanBlock(buffer, key, numBlockEntries)
    }

    // For larger blocks, use binary search approach
    binarySearchBlock(buffer, blockOffset.toInt + 4, key, numBlockEntries)
  }

  /**
   * Linear scan for small blocks (better cache locality).
   */
  private def linearScanBlock(
      buffer: MappedByteBuffer,
      key: Array[Byte],
      numEntries: Int): MemTableEntry = {
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
      if (cmp == 0) {
        return MemTableEntry(if (isDeleted) null else entryValue, seqNum)
      } else if (cmp > 0) {
        return null
      }
    }
    null
  }

  /**
   * Binary search for larger blocks (O(log n) comparisons).
   */
  private def binarySearchBlock(
      buffer: MappedByteBuffer,
      blockStart: Int,
      key: Array[Byte],
      numEntries: Int): MemTableEntry = {
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
   * Iterator over all entries in this SSTable.
   */
  def iterator: Iterator[(Array[Byte], MemTableEntry)] = {
    new SSTableIterator(this)
  }

  /**
   * Iterator starting from a specific key.
   * Uses sparse index to seek to the approximate position.
   */
  def iteratorFrom(fromKey: Array[Byte]): Iterator[(Array[Byte], MemTableEntry)] = {
    new SSTableSeekIterator(this, fromKey, null)
  }

  /**
   * Iterator for keys with a specific prefix.
   * Uses sparse index to seek to prefix start and stops when prefix no longer matches.
   */
  def prefixIterator(prefix: Array[Byte]): Iterator[(Array[Byte], MemTableEntry)] = {
    new SSTableSeekIterator(this, prefix, prefix)
  }

  /**
   * Get the size of this SSTable in bytes.
   */
  def size: Long = file.length()

  /**
   * Get the estimated memory used by the Bloom filter.
   */
  def bloomFilterMemory: Long = bloomFilter.estimatedMemory

  override def close(): Unit = {
    synchronized {
      if (mappedBuffer != null) {
        // Force unmap (helps with file deletion on Windows)
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
   * Delete the SSTable file.
   */
  def delete(): Unit = {
    close()
    file.delete()
  }
}

object SSTable extends Logging {

  private val MAGIC_NUMBER = 0x5354424C // "STBL"
  private val VERSION = 1

  /**
   * Create an SSTable from a MemTable.
   */
  def createFromMemTable(
      memTable: MemTable,
      directory: File,
      blockSize: Int,
      bloomFilterFpp: Double,
      sparseIndexInterval: Int): SSTable = {

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
   * Create an SSTable from a sequence of entries.
   */
  def createFromEntries(
      entries: Iterable[(Array[Byte], MemTableEntry)],
      directory: File,
      blockSize: Int,
      bloomFilterFpp: Double,
      sparseIndexInterval: Int): SSTable = {

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

  // createFromEntriesInternal: Streaming SSTable creation (memory-efficient)
  // This method streams data directly to disk instead of buffering in memory.
  // Memory usage is O(bloom_filter + sparse_index) regardless of data size.
  // Uses CountingOutputStream to track byte positions without buffering.
  private def createFromEntriesInternal(
      entries: Iterator[(Array[Byte], MemTableEntry)],
      numEntries: Int,
      directory: File,
      blockSize: Int,
      bloomFilterFpp: Double,
      sparseIndexInterval: Int): SSTable = {

    if (numEntries == 0) {
      throw new IllegalArgumentException("Cannot create empty SSTable")
    }

    val file = new File(directory, s"sstable_${UUID.randomUUID()}.sst")
    // Use 256KB buffer for optimal sequential write performance
    val fileOutputStream = new FileOutputStream(file)
    val bufferedOutput = new BufferedOutputStream(fileOutputStream, 256 * 1024)
    // CountingOutputStream tracks bytes written without buffering
    val countingOutput = new CountingOutputStream(bufferedOutput)
    val dataOutput = new DataOutputStream(countingOutput)

    try {
      // Build Bloom filter incrementally (memory: ~1.2 bytes per key at 1% FPP)
      val bloomFilter = LSMBloomFilter(numEntries, bloomFilterFpp)
      // Sparse index entries (memory: ~24 bytes per entry, one per sparseIndexInterval keys)
      val sparseIndex = new SparseIndex()

      var currentBlockSize = 0
      var currentBlockEntries = 0
      var currentBlockStart = 0L
      var firstKeyInBlock: Array[Byte] = null
      var minKey: Array[Byte] = null
      var maxKey: Array[Byte] = null
      var entryCount = 0L
      var indexCounter = 0

      // Stream entries directly to disk - no buffering of data
      for ((key, entry) <- entries) {
        // Update min/max keys (only clone first and last)
        if (minKey == null) minKey = key.clone()
        maxKey = key // Will clone at the end

        // Add to Bloom filter (O(k) hash operations, k = number of hash functions)
        bloomFilter.put(key)

        // Calculate entry size for block management
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
        if (firstKeyInBlock == null) {
          firstKeyInBlock = key.clone()
        }

        // Write entry directly to disk (through buffer)
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
      if (firstKeyInBlock != null) {
        sparseIndex.addEntry(firstKeyInBlock, currentBlockStart)
      }

      // Clone maxKey now (we only kept reference during iteration)
      maxKey = maxKey.clone()

      // Flush data section and record size
      dataOutput.flush()
      val dataSize = countingOutput.getBytesWritten

      // Write Bloom filter (serialized, typically small)
      val bloomBytes = bloomFilter.toBytes()
      bufferedOutput.write(bloomBytes)

      // Write sparse index (serialized, typically small)
      val indexBytes = sparseIndex.toBytes()
      bufferedOutput.write(indexBytes)

      // Write footer with metadata
      val footerOutput = new DataOutputStream(bufferedOutput)
      footerOutput.writeInt(MAGIC_NUMBER)
      footerOutput.writeInt(VERSION)
      footerOutput.writeLong(dataSize)
      footerOutput.writeInt(bloomBytes.length)
      footerOutput.writeInt(indexBytes.length)
      footerOutput.writeLong(entryCount)
      footerOutput.writeInt(minKey.length)
      footerOutput.write(minKey)
      footerOutput.writeInt(maxKey.length)
      footerOutput.write(maxKey)

      footerOutput.flush()
      bufferedOutput.flush()

      new SSTable(
        file,
        dataSize,
        bloomFilter,
        sparseIndex,
        minKey,
        maxKey,
        entryCount
      )

    } finally {
      // Close all streams (closing outer stream closes inner ones)
      bufferedOutput.close()
    }
  }

  /**
   * Load an existing SSTable from file.
   */
  def load(file: File): SSTable = {
    val raf = new RandomAccessFile(file, "r")
    try {
      // Read footer (last 48+ bytes)
      val footerSize = 4 + 4 + 8 + 4 + 4 + 8 // Fixed part of footer
      raf.seek(file.length() - footerSize)

      val magic = raf.readInt()
      if (magic != MAGIC_NUMBER) {
        throw new IOException(s"Invalid SSTable magic number: $magic")
      }

      val version = raf.readInt()
      if (version != VERSION) {
        throw new IOException(s"Unsupported SSTable version: $version")
      }

      val dataSize = raf.readLong()
      val bloomSize = raf.readInt()
      val indexSize = raf.readInt()
      val numEntries = raf.readLong()

      // Read min/max keys (at end of footer)
      val minKeyLen = raf.readInt()
      val minKey = new Array[Byte](minKeyLen)
      raf.readFully(minKey)

      val maxKeyLen = raf.readInt()
      val maxKey = new Array[Byte](maxKeyLen)
      raf.readFully(maxKey)

      // Read Bloom filter
      raf.seek(dataSize)
      val bloomBytes = new Array[Byte](bloomSize)
      raf.readFully(bloomBytes)
      val bloomFilter = LSMBloomFilter.fromBytes(bloomBytes)

      // Read sparse index
      val indexBytes = new Array[Byte](indexSize)
      raf.readFully(indexBytes)
      val sparseIndex = SparseIndex.fromBytes(indexBytes)

      new SSTable(
        file,
        dataSize,
        bloomFilter,
        sparseIndex,
        minKey,
        maxKey,
        numEntries
      )

    } finally {
      raf.close()
    }
  }
}

/**
 * Iterator over entries in an SSTable.
 */
private class SSTableIterator(ssTable: SSTable) extends Iterator[(Array[Byte], MemTableEntry)] {

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

  // Prefetch first entry
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
 * Iterator that seeks to a starting key and optionally stops at a prefix boundary.
 *
 * Uses the sparse index to efficiently seek to the approximate position,
 * then scans forward to find the exact starting point.
 *
 * @param ssTable The SSTable to iterate
 * @param seekKey The key to seek to (inclusive)
 * @param prefixFilter If non-null, stops iteration when key no longer has this prefix
 */
private class SSTableSeekIterator(
    ssTable: SSTable,
    seekKey: Array[Byte],
    prefixFilter: Array[Byte]) extends Iterator[(Array[Byte], MemTableEntry)] {

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
 * Bloom filter optimized for LSM-Tree use cases.
 *
 * Uses a simple but effective implementation based on multiple hash functions
 * derived from two base hashes (Kirsch-Mitzenmacher optimization).
 */
class LSMBloomFilter private[state] (
    private val numBits: Int,
    private val numHashFunctions: Int,
    private val bitArray: Array[Long]) {

  /**
   * Add a key to the Bloom filter.
   */
  def put(key: Array[Byte]): Unit = {
    val (h1, h2) = hash(key)
    for (i <- 0 until numHashFunctions) {
      val bit = ((h1 + i.toLong * h2) & Long.MaxValue) % numBits
      val longIndex = (bit / 64).toInt
      val bitIndex = (bit % 64).toInt
      bitArray(longIndex) |= (1L << bitIndex)
    }
  }

  /**
   * Check if a key might be in the set.
   *
   * @return true if the key might be present, false if definitely not present
   */
  def mightContain(key: Array[Byte]): Boolean = {
    val (h1, h2) = hash(key)
    for (i <- 0 until numHashFunctions) {
      val bit = ((h1 + i.toLong * h2) & Long.MaxValue) % numBits
      val longIndex = (bit / 64).toInt
      val bitIndex = (bit % 64).toInt
      if ((bitArray(longIndex) & (1L << bitIndex)) == 0) {
        return false
      }
    }
    true
  }

  /**
   * Serialize to bytes.
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
   * Estimated memory usage.
   */
  def estimatedMemory: Long = bitArray.length * 8L + 16

  /**
   * Hash function using MurmurHash3.
   */
  private def hash(key: Array[Byte]): (Long, Long) = {
    val h1 = MurmurHash3.hash64(key, 0, key.length, 0x9747b28c)
    val h2 = MurmurHash3.hash64(key, 0, key.length, 0xe17a1465)
    (h1, h2)
  }
}

object LSMBloomFilter {

  /**
   * Create a new Bloom filter.
   *
   * @param expectedInsertions Expected number of insertions
   * @param fpp False positive probability (e.g., 0.01 for 1%)
   */
  def apply(expectedInsertions: Int, fpp: Double): LSMBloomFilter = {
    // Optimal number of bits: -n * ln(p) / (ln(2)^2)
    val numBits = math.ceil(-expectedInsertions * math.log(fpp) /
      (math.log(2) * math.log(2))).toInt.max(64)

    // Optimal number of hash functions: (m/n) * ln(2)
    val numHashFunctions = math.ceil(numBits.toDouble /
      expectedInsertions * math.log(2)).toInt.max(1)

    val numLongs = (numBits + 63) / 64
    val bitArray = new Array[Long](numLongs)

    new LSMBloomFilter(numBits, numHashFunctions, bitArray)
  }

  /**
   * Deserialize from bytes.
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
 * MurmurHash3 implementation for Bloom filter hashing.
 */
object MurmurHash3 {

  private val C1: Long = 0x87c37b91114253d5L
  private val C2: Long = 0x4cf5ad432745937fL

  def hash64(data: Array[Byte], offset: Int, length: Int, seed: Int): Long = {
    var h = seed.toLong
    val nblocks = length / 8

    // Body
    for (i <- 0 until nblocks) {
      val k = getLongLittleEndian(data, offset + i * 8)
      h = mix64(h, k)
    }

    // Tail
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
 * Sparse index for efficient block lookup in SSTables.
 *
 * Maps keys to block offsets at regular intervals.
 */
class SparseIndex {

  import LSMTree.compareByteArrays

  private val entries = new ArrayBuffer[(Array[Byte], Long)]()

  /**
   * Add an index entry.
   */
  def addEntry(key: Array[Byte], offset: Long): Unit = {
    entries += ((key.clone(), offset))
  }

  /**
   * Find the block that might contain the given key.
   *
   * @return Block offset, or -1 if key is smaller than all indexed keys
   */
  def findBlock(key: Array[Byte]): Long = {
    if (entries.isEmpty) return -1

    // Binary search for the largest entry <= key
    var low = 0
    var high = entries.length - 1
    var result = -1L

    while (low <= high) {
      val mid = (low + high) / 2
      val cmp = compareByteArrays(entries(mid)._1, key)

      if (cmp <= 0) {
        result = entries(mid)._2
        low = mid + 1
      } else {
        high = mid - 1
      }
    }

    result
  }

  /**
   * Get the number of index entries.
   */
  def size: Int = entries.length

  /**
   * Serialize to bytes.
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

object SparseIndex {

  /**
   * Deserialize from bytes.
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

// =============================================================================
// CountingOutputStream - Tracks bytes written without buffering
// =============================================================================
// A lightweight wrapper that counts bytes written to the underlying stream.
// Used during SSTable creation to track file positions without buffering
// all data in memory. Zero overhead - just increments a counter.
// =============================================================================

/**
 * An OutputStream wrapper that counts bytes written.
 * This allows tracking file position without buffering data in memory.
 *
 * @param out The underlying output stream to write to
 */
private[state] class CountingOutputStream(out: OutputStream) extends OutputStream {

  private var bytesWritten: Long = 0L

  /** Get the total number of bytes written so far */
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
