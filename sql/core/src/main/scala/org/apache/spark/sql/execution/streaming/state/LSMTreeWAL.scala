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
import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.ReentrantLock
import java.util.zip.CRC32
import javax.annotation.concurrent.ThreadSafe

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

/**
 * Write-Ahead Log (WAL) for LSM-Tree durability.
 *
 * The WAL ensures durability by writing all operations to disk before
 * they are applied to the MemTable. In case of failure, the WAL can be
 * replayed to recover uncommitted data.
 *
 * WAL file format:
 * {{{
 *   | Header (16 bytes) |
 *   | Record 1 |
 *   | Record 2 |
 *   | ... |
 *
 *   Header:
 *   | Magic (4 bytes) | Version (4 bytes) | Sequence Start (8 bytes) |
 *
 *   Record:
 *   | Length (4 bytes) | Type (1 byte) | Key Length (4 bytes) | Key |
 *   | Value Length (4 bytes) | Value | CRC32 (4 bytes) |
 * }}}
 *
 * @param directory Directory for WAL files
 * @param loggingId Identifier for logging
 */
@ThreadSafe
class WriteAheadLog(
    directory: File,
    loggingId: String) extends Closeable with Logging {

  import WriteAheadLog._
  import WALOperation._

  // Ensure directory exists
  if (!directory.exists()) {
    directory.mkdirs()
  }

  // Current WAL file and streams
  @volatile private var currentFile: File = _
  @volatile private var outputStream: DataOutputStream = _
  @volatile private var fileOutputStream: FileOutputStream = _

  // Sequence tracking
  private val sequenceNumber = new AtomicLong(0L)
  private val commitSequence = new AtomicLong(0L)

  // Write lock for thread safety
  private val writeLock = new ReentrantLock()

  // Closed flag
  private val closed = new AtomicBoolean(false)

  // Total WAL size
  private val totalSize = new AtomicLong(0L)

  // Initialize with a new WAL file
  rotateWALFile()

  /**
   * Append an operation to the WAL.
   */
  def append(operation: WALOperation, key: Array[Byte], value: Array[Byte]): Long = {
    if (closed.get()) {
      throw new IllegalStateException("WAL is closed")
    }

    writeLock.lock()
    try {
      val seq = sequenceNumber.incrementAndGet()
      val record = encodeRecord(operation, key, value, seq)

      outputStream.write(record)
      totalSize.addAndGet(record.length)

      // Rotate if file is too large (256 MB per file)
      if (totalSize.get() > MAX_FILE_SIZE) {
        rotateWALFile()
      }

      seq
    } finally {
      writeLock.unlock()
    }
  }

  /**
   * Sync WAL to disk.
   */
  def sync(): Unit = {
    if (closed.get()) return

    writeLock.lock()
    try {
      if (outputStream != null) {
        outputStream.flush()
        fileOutputStream.getFD.sync()
        commitSequence.set(sequenceNumber.get())
      }
    } finally {
      writeLock.unlock()
    }
  }

  /**
   * Truncate WAL to last committed position.
   */
  def truncate(): Unit = {
    writeLock.lock()
    try {
      // For simplicity, we just rotate to a new file
      // In a production system, you'd truncate the current file
      rotateWALFile()
      sequenceNumber.set(commitSequence.get())
    } finally {
      writeLock.unlock()
    }
  }

  /**
   * Replay WAL records in a version range.
   */
  def replay(
      fromVersion: Long,
      toVersion: Long)(
      callback: (WALOperation, Array[Byte], Array[Byte]) => Unit): Unit = {

    val startTime = System.currentTimeMillis()
    val walFiles = getWALFiles.sortBy(_.getName)

    logInfo(s"[$loggingId] WAL REPLAY START: fromVersion=$fromVersion, toVersion=$toVersion, " +
      s"walFiles=${walFiles.length}")

    var totalEntries = 0L
    for (file <- walFiles) {
      try {
        val entriesBefore = totalEntries
        totalEntries += replayFile(file, fromVersion, toVersion, callback)
        if (totalEntries > entriesBefore) {
          logDebug(s"[$loggingId] Replayed ${totalEntries - entriesBefore} entries from ${file.getName}")
        }
      } catch {
        case NonFatal(e) =>
          logWarning(s"[$loggingId] Error replaying WAL file ${file.getName}", e)
      }
    }

    val duration = System.currentTimeMillis() - startTime
    logInfo(s"[$loggingId] WAL REPLAY COMPLETE: entries=$totalEntries, duration=${duration}ms")
  }

  /**
   * Clean up old WAL files.
   */
  def cleanup(minVersionToKeep: Long): Unit = {
    writeLock.lock()
    try {
      val walFiles = getWALFiles
      for (file <- walFiles) {
        if (file != currentFile) {
          try {
            val maxSeq = getMaxSequenceFromFile(file)
            if (maxSeq < minVersionToKeep) {
              file.delete()
              logInfo(s"[$loggingId] Deleted old WAL file ${file.getName}")
            }
          } catch {
            case NonFatal(e) =>
              logWarning(s"[$loggingId] Error cleaning WAL file ${file.getName}", e)
          }
        }
      }
    } finally {
      writeLock.unlock()
    }
  }

  /**
   * Get total WAL size.
   */
  def size: Long = {
    getWALFiles.map(_.length()).sum
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      writeLock.lock()
      try {
        if (outputStream != null) {
          outputStream.flush()
          outputStream.close()
          outputStream = null
        }
        if (fileOutputStream != null) {
          fileOutputStream.close()
          fileOutputStream = null
        }
      } finally {
        writeLock.unlock()
      }
      logInfo(s"[$loggingId] WAL closed")
    }
  }

  // ============================================================================
  // Internal Methods
  // ============================================================================

  private def rotateWALFile(): Unit = {
    writeLock.lock()
    try {
      // Close current file
      if (outputStream != null) {
        outputStream.flush()
        outputStream.close()
      }
      if (fileOutputStream != null) {
        fileOutputStream.close()
      }

      // Create new file
      val timestamp = System.currentTimeMillis()
      val seq = sequenceNumber.get()
      currentFile = new File(directory, f"wal_${timestamp}_$seq%020d.log")

      fileOutputStream = new FileOutputStream(currentFile, false)
      outputStream = new DataOutputStream(new BufferedOutputStream(fileOutputStream, 64 * 1024))

      // Write header
      outputStream.writeInt(MAGIC_NUMBER)
      outputStream.writeInt(VERSION)
      outputStream.writeLong(seq)
      outputStream.flush()

      totalSize.set(HEADER_SIZE)

      logInfo(s"[$loggingId] Created new WAL file ${currentFile.getName}")
    } finally {
      writeLock.unlock()
    }
  }

  private def encodeRecord(
      operation: WALOperation,
      key: Array[Byte],
      value: Array[Byte],
      seq: Long): Array[Byte] = {

    val valueLen = if (value != null) value.length else -1
    val recordSize = 1 + 4 + key.length + 4 +
      (if (value != null) value.length else 0) + 8 + 4

    val buffer = ByteBuffer.allocate(4 + recordSize)

    // Length
    buffer.putInt(recordSize)

    // Record content
    val contentStart = buffer.position()
    buffer.put(operation.id.toByte)
    buffer.putInt(key.length)
    buffer.put(key)
    buffer.putInt(valueLen)
    if (value != null) {
      buffer.put(value)
    }
    buffer.putLong(seq)

    // CRC32
    val crc = new CRC32()
    crc.update(buffer.array(), contentStart, recordSize - 4)
    buffer.putInt(crc.getValue.toInt)

    buffer.array()
  }

  private def replayFile(
      file: File,
      fromVersion: Long,
      toVersion: Long,
      callback: (WALOperation, Array[Byte], Array[Byte]) => Unit): Long = {

    val input = new DataInputStream(
      new BufferedInputStream(new FileInputStream(file), 64 * 1024))

    var entriesReplayed = 0L

    try {
      // Read header
      val magic = input.readInt()
      if (magic != MAGIC_NUMBER) {
        throw new IOException(s"Invalid WAL magic number: $magic")
      }

      val version = input.readInt()
      if (version != VERSION) {
        throw new IOException(s"Unsupported WAL version: $version")
      }

      // Read records
      var continue = true
      while (input.available() > 0 && continue) {
        val recordLen = input.readInt()
        if (recordLen <= 0) {
          continue = false
        } else {
          val recordData = new Array[Byte](recordLen)
          input.readFully(recordData)

          val record = ByteBuffer.wrap(recordData)

          val opByte = record.get()
          val operation = WALOperation(opByte.toInt)

          val keyLen = record.getInt()
          val key = new Array[Byte](keyLen)
          record.get(key)

          val valueLen = record.getInt()
          val value = if (valueLen >= 0) {
            val v = new Array[Byte](valueLen)
            record.get(v)
            v
          } else {
            null
          }

          val seq = record.getLong()
          val storedCrc = record.getInt()

          // Verify CRC
          val crc = new CRC32()
          crc.update(recordData, 0, recordLen - 4)
          if (crc.getValue.toInt != storedCrc) {
            logWarning(s"[$loggingId] CRC mismatch in WAL record, seq=$seq")
            // Skip this record, continue to next
          } else {
            // Apply if in version range
            if (seq > fromVersion && seq <= toVersion) {
              callback(operation, key, value)
              entriesReplayed += 1
            }
          }
        }
      }
    } finally {
      input.close()
    }

    entriesReplayed
  }

  private def getWALFiles: Array[File] = {
    val files = directory.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.startsWith("wal_") && name.endsWith(".log")
      }
    })
    if (files == null) Array.empty else files
  }

  private def getMaxSequenceFromFile(file: File): Long = {
    // Extract sequence from filename
    val name = file.getName
    val parts = name.stripPrefix("wal_").stripSuffix(".log").split("_")
    if (parts.length >= 2) {
      try {
        parts(1).toLong
      } catch {
        case _: NumberFormatException => 0L
      }
    } else {
      0L
    }
  }

  override protected def logName: String = s"${super.logName} $loggingId"
}

object WriteAheadLog {
  val MAGIC_NUMBER: Int = 0x57414C00 // "WAL\0"
  val VERSION: Int = 1
  private val HEADER_SIZE: Int = 16
  private val MAX_FILE_SIZE: Long = 256 * 1024 * 1024 // 256 MB
}

/**
 * Statistics collector for LSM-Tree.
 * Tracks read/write operations, cache hits, and performance metrics.
 */
class LSMTreeStatsCollector {

  private val _reads = new AtomicLong(0L)
  private val _writes = new AtomicLong(0L)
  private val _memTableHits = new AtomicLong(0L)
  private val _diskReads = new AtomicLong(0L)
  private val _bloomFilterChecks = new AtomicLong(0L)
  private val _bloomFilterSkips = new AtomicLong(0L)
  private val _lastCompactionTimeMs = new AtomicLong(0L)
  private val _bloomFilterMemory = new AtomicLong(0L)
  private val _estimatedKeyCount = new AtomicLong(0L)

  // Increment methods
  def incrementReads(): Unit = _reads.incrementAndGet()
  def incrementWrites(): Unit = {
    _writes.incrementAndGet()
    _estimatedKeyCount.incrementAndGet()
  }
  def incrementMemTableHits(): Unit = _memTableHits.incrementAndGet()
  def incrementDiskReads(): Unit = _diskReads.incrementAndGet()
  def incrementBloomFilterChecks(): Unit = _bloomFilterChecks.incrementAndGet()
  def incrementBloomFilterSkips(): Unit = _bloomFilterSkips.incrementAndGet()

  def recordCompactionTime(ms: Long): Unit = _lastCompactionTimeMs.set(ms)
  def addBloomFilterMemory(bytes: Long): Unit = _bloomFilterMemory.addAndGet(bytes)

  // Getter methods for logging and metrics
  def reads: Long = _reads.get()
  def writes: Long = _writes.get()
  def memTableHits: Long = _memTableHits.get()
  def diskReads: Long = _diskReads.get()
  def bloomFilterChecks: Long = _bloomFilterChecks.get()
  def bloomFilterSkips: Long = _bloomFilterSkips.get()
  def estimatedKeyCount: Long = _estimatedKeyCount.get()
  def bloomFilterMemory: Long = _bloomFilterMemory.get()
  def lastCompactionTimeMs: Long = _lastCompactionTimeMs.get()

  def bloomFilterHitRatePercent: Long = {
    val checks = _bloomFilterChecks.get()
    val skips = _bloomFilterSkips.get()
    if (checks == 0) 0L
    else (skips * 100L) / checks
  }

  /**
   * Get a summary string for logging.
   */
  def summary: String = {
    s"reads=$reads, writes=$writes, memTableHits=$memTableHits, diskReads=$diskReads, " +
      s"bloomFilterHitRate=${bloomFilterHitRatePercent}%"
  }

  /**
   * Reset all counters (useful for per-batch metrics).
   */
  def reset(): Unit = {
    _reads.set(0L)
    _writes.set(0L)
    _memTableHits.set(0L)
    _diskReads.set(0L)
    _bloomFilterChecks.set(0L)
    _bloomFilterSkips.set(0L)
  }
}
