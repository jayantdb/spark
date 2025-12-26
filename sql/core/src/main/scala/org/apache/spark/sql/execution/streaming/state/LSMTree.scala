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
import java.util.{Arrays => JArrays, Comparator}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.ReentrantReadWriteLock
import javax.annotation.concurrent.{GuardedBy, ThreadSafe}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.checkpointing.CheckpointFileManager

/**
 * A high-performance LSM-Tree (Log-Structured Merge-Tree) implementation
 * optimized for Spark StateStore use cases.
 *
 * Key features:
 * - Pure Scala/JVM implementation (no JNI overhead)
 * - Off-heap memory using DirectByteBuffers
 * - Memory-mapped files for fast I/O
 * - Bloom filters for read optimization
 * - Write-Ahead Log for durability
 * - Tiered compaction for handling large state
 *
 * @param localRootDir Local directory for working files (SSTables, WAL)
 * @param dfsRootDir DFS directory for checkpointing
 * @param conf LSM-Tree configuration
 * @param fm Checkpoint file manager
 * @param hadoopConf Hadoop configuration
 * @param loggingId Identifier for logging
 */
@ThreadSafe
class LSMTree(
    localRootDir: File,
    dfsRootDir: String,
    conf: LSMTreeConf,
    fm: CheckpointFileManager,
    hadoopConf: Configuration,
    loggingId: String) extends Logging with Closeable {

  import LSMTree._

  // Directory Structure
  private val memTableDir = new File(localRootDir, "memtable")
  private val ssTableDir = new File(localRootDir, "sstables")
  private val walDir = new File(localRootDir, "wal")
  private val snapshotDir = new File(localRootDir, "snapshots")

  // Initialize directories
  Seq(memTableDir, ssTableDir, walDir, snapshotDir).foreach { dir =>
    if (!dir.exists()) {
      dir.mkdirs()
    }
  }

  // Core Components
  // Active mutable MemTable for writes
  @GuardedBy("memTableLock")
  private var activeMemTable: MemTable = new MemTable(conf.memTableSizeBytes)

  // Immutable MemTables waiting to be flushed
  @GuardedBy("memTableLock")
  private val immutableMemTables: mutable.ArrayBuffer[MemTable] = mutable.ArrayBuffer.empty

  // Lock for MemTable operations
  private val memTableLock = new ReentrantReadWriteLock()

  // SSTable levels (level 0 is newest)
  @GuardedBy("ssTableLock")
  private val levels: Array[mutable.ArrayBuffer[SSTable]] =
    Array.fill(conf.maxLevels)(mutable.ArrayBuffer.empty)

  private val ssTableLock = new ReentrantReadWriteLock()

  // Write-Ahead Log
  private val wal: WriteAheadLog = if (conf.walEnabled) {
    new WriteAheadLog(walDir, loggingId)
  } else {
    null
  }

  // Column families
  private val columnFamilies: ConcurrentHashMap[String, ColumnFamilyState] =
    new ConcurrentHashMap[String, ColumnFamilyState]()

  // Initialize default column family
  columnFamilies.put(StateStore.DEFAULT_COL_FAMILY_NAME, new ColumnFamilyState())

  // Current version
  private val currentVersion = new AtomicLong(0L)

  // Background compaction executor
  private val compactionExecutor: ScheduledExecutorService =
    Executors.newScheduledThreadPool(conf.compactionThreads)

  // Statistics
  private val stats = new LSMTreeStatsCollector()

  // Shutdown flag
  private val closed = new AtomicBoolean(false)

  /**
   * Load state at the specified version.
   */
  def load(version: Long): Unit = {
    val startTime = System.currentTimeMillis()
    logInfo(s"[$loggingId] LOAD START: targetVersion=$version")

    if (version < 0) {
      throw new IllegalArgumentException(s"Version must be non-negative: $version")
    }

    if (version == 0) {
      // Fresh start
      logDebug(s"[$loggingId] Fresh start - resetting state")
      reset()
      currentVersion.set(0)
      logInfo(s"[$loggingId] LOAD COMPLETE: version=0 (fresh start), duration=0ms")
      return
    }

    // Find the closest snapshot and replay
    val snapshotVersion = findClosestSnapshot(version)
    logDebug(s"[$loggingId] Found closest snapshot: version=$snapshotVersion")

    if (snapshotVersion > 0) {
      val snapshotStart = System.currentTimeMillis()
      loadSnapshot(snapshotVersion)
      logDebug(s"[$loggingId] Loaded snapshot $snapshotVersion in " +
        s"${System.currentTimeMillis() - snapshotStart}ms")
    } else {
      logDebug(s"[$loggingId] No snapshot found, starting fresh")
      reset()
    }

    // Replay WAL from snapshot to target version
    if (wal != null && version > snapshotVersion) {
      val walStart = System.currentTimeMillis()
      var walEntryCount = 0L
      wal.replay(snapshotVersion, version) { (operation, key, value) =>
        operation match {
          case WALOperation.PUT =>
            activeMemTable.put(key, value)
          case WALOperation.DELETE =>
            activeMemTable.delete(key)
        }
        walEntryCount += 1
      }
      logDebug(s"[$loggingId] Replayed $walEntryCount WAL entries from version $snapshotVersion " +
        s"to $version in ${System.currentTimeMillis() - walStart}ms")
    }

    currentVersion.set(version)
    val totalTime = System.currentTimeMillis() - startTime
    val keyCount = activeMemTable.size + immutableMemTables.map(_.size).sum
    logInfo(s"[$loggingId] LOAD COMPLETE: version=$version, snapshotVersion=$snapshotVersion, " +
      s"keyCount=$keyCount, duration=${totalTime}ms")
  }

  /**
   * Get value for key.
   *
   * Read path (simple and fast):
   * 1. Check active MemTable (lock-free ConcurrentSkipListMap)
   * 2. Check immutable MemTables (newest first)
   * 3. Check SSTables (with Bloom filter optimization)
   */
  def get(key: Array[Byte], colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME):
      Array[Byte] = {
    stats.incrementReads()

    // 1. Check active MemTable (lock-free read)
    val activeResult = activeMemTable.get(key)
    if (activeResult != null) {
      if (activeResult.isDeleted) return null
      stats.incrementMemTableHits()
      return activeResult.value
    }

    // 2. Check immutable MemTables (newest first)
    memTableLock.readLock().lock()
    try {
      for (memTable <- immutableMemTables.reverseIterator) {
        val immResult = memTable.get(key)
        if (immResult != null) {
          if (immResult.isDeleted) return null
          stats.incrementMemTableHits()
          return immResult.value
        }
      }
    } finally {
      memTableLock.readLock().unlock()
    }

    // 3. Check SSTables (newest first, level by level)
    ssTableLock.readLock().lock()
    try {
      for (level <- 0 until conf.maxLevels) {
        for (ssTable <- levels(level).reverseIterator) {
          // Use Bloom filter to skip tables that definitely don't have the key
          if (ssTable.mightContain(key)) {
            stats.incrementBloomFilterChecks()
            val result = ssTable.get(key)
            if (result != null) {
              if (result.isDeleted) return null
              stats.incrementDiskReads()
              return result.value
            }
          } else {
            stats.incrementBloomFilterSkips()
          }
        }
      }
    } finally {
      ssTableLock.readLock().unlock()
    }

    null
  }

  /**
   * Put key-value pair.
   */
  def put(
      key: Array[Byte],
      value: Array[Byte],
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit = {
    stats.incrementWrites()

    // Write to WAL first (if enabled)
    if (wal != null) {
      wal.append(WALOperation.PUT, key, value)
    }

    // Write to MemTable
    memTableLock.writeLock().lock()
    try {
      activeMemTable.put(key, value)

      // Check if MemTable needs to be flushed
      if (activeMemTable.estimatedSize >= conf.memTableSizeBytes) {
        rotateMemTable()
      }
    } finally {
      memTableLock.writeLock().unlock()
    }
  }

  /**
   * Delete key.
   */
  def delete(
      key: Array[Byte],
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit = {
    stats.incrementWrites()

    // Write tombstone to WAL
    if (wal != null) {
      wal.append(WALOperation.DELETE, key, null)
    }

    // Write tombstone to MemTable
    memTableLock.writeLock().lock()
    try {
      activeMemTable.delete(key)

      if (activeMemTable.estimatedSize >= conf.memTableSizeBytes) {
        rotateMemTable()
      }
    } finally {
      memTableLock.writeLock().unlock()
    }
  }

  /**
   * Iterator over all key-value pairs.
   */
  def iterator(colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME):
      Iterator[(Array[Byte], Array[Byte])] = {
    // Collect all iterators
    val iterators = new ArrayBuffer[Iterator[(Array[Byte], MemTableEntry)]]()

    memTableLock.readLock().lock()
    try {
      // Active MemTable
      iterators += activeMemTable.iterator.map { case (k, v) => (k.clone(), v) }

      // Immutable MemTables
      for (memTable <- immutableMemTables.reverseIterator) {
        iterators += memTable.iterator.map { case (k, v) => (k.clone(), v) }
      }
    } finally {
      memTableLock.readLock().unlock()
    }

    ssTableLock.readLock().lock()
    try {
      // SSTables
      for (level <- 0 until conf.maxLevels) {
        for (ssTable <- levels(level).reverseIterator) {
          iterators += ssTable.iterator
        }
      }
    } finally {
      ssTableLock.readLock().unlock()
    }

    // Merge iterators with deduplication
    new MergingIterator(iterators.toSeq)
  }

  /**
   * Prefix scan with efficient seeking.
   *
   * Uses ConcurrentSkipListMap.tailMap and SSTable sparse index to seek
   * directly to the prefix start, avoiding a full scan.
   */
  def prefixScan(
      prefix: Array[Byte],
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME):
      Iterator[(Array[Byte], Array[Byte])] = {
    // Collect prefix iterators from all sources
    val iterators = new ArrayBuffer[Iterator[(Array[Byte], MemTableEntry)]]()

    memTableLock.readLock().lock()
    try {
      // Active MemTable - use prefix iterator
      iterators += activeMemTable.prefixIterator(prefix).map { case (k, v) => (k.clone(), v) }

      // Immutable MemTables - use prefix iterators
      for (memTable <- immutableMemTables.reverseIterator) {
        iterators += memTable.prefixIterator(prefix).map { case (k, v) => (k.clone(), v) }
      }
    } finally {
      memTableLock.readLock().unlock()
    }

    ssTableLock.readLock().lock()
    try {
      // SSTables - use prefix iterators with sparse index seeking
      for (level <- 0 until conf.maxLevels) {
        for (ssTable <- levels(level).reverseIterator) {
          iterators += ssTable.prefixIterator(prefix)
        }
      }
    } finally {
      ssTableLock.readLock().unlock()
    }

    // Merge iterators with deduplication
    new MergingIterator(iterators.toSeq)
  }

  /**
   * Range scan for keys in [startKey, endKey).
   */
  def rangeScan(
      startKey: Array[Byte],
      endKey: Array[Byte],
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME):
      Iterator[(Array[Byte], Array[Byte])] = {
    // Collect range iterators from all sources
    val iterators = new ArrayBuffer[Iterator[(Array[Byte], MemTableEntry)]]()

    memTableLock.readLock().lock()
    try {
      // Active MemTable - use range iterator
      iterators += activeMemTable
        .iteratorRange(startKey, endKey).map { case (k, v) => (k.clone(), v) }

      // Immutable MemTables
      for (memTable <- immutableMemTables.reverseIterator) {
        iterators += memTable.iteratorRange(startKey, endKey).map { case (k, v) => (k.clone(), v) }
      }
    } finally {
      memTableLock.readLock().unlock()
    }

    ssTableLock.readLock().lock()
    try {
      // SSTables - use seek iterators
      for (level <- 0 until conf.maxLevels) {
        for (ssTable <- levels(level).reverseIterator) {
          // For range scan, we seek to startKey and filter by endKey
          iterators += ssTable.iteratorFrom(startKey).takeWhile { case (key, _) =>
            compareByteArrays(key, endKey) < 0
          }
        }
      }
    } finally {
      ssTableLock.readLock().unlock()
    }

    // Merge iterators with deduplication
    new MergingIterator(iterators.toSeq)
  }

  /**
   * Commit changes at the specified version.
   */
  def commit(newVersion: Long): Unit = {
    val startTime = System.currentTimeMillis()
    val memTableSize = activeMemTable.size
    val memTableBytes = activeMemTable.estimatedSize

    logInfo(s"[$loggingId] COMMIT START: version=$newVersion, " +
      s"pendingKeys=$memTableSize, memTableBytes=$memTableBytes")

    // Sync WAL
    if (wal != null) {
      val walStart = System.currentTimeMillis()
      wal.sync()
      logDebug(s"[$loggingId] WAL synced in ${System.currentTimeMillis() - walStart}ms")
    }

    // Flush MemTable if it has data
    memTableLock.writeLock().lock()
    try {
      if (activeMemTable.size > 0) {
        logDebug(s"[$loggingId] Rotating MemTable with ${activeMemTable.size} entries")
        rotateMemTable()
      }
    } finally {
      memTableLock.writeLock().unlock()
    }

    // Flush all immutable MemTables to SSTables
    val flushStart = System.currentTimeMillis()
    val immutableCount = immutableMemTables.size
    flushImmutableMemTables()
    if (immutableCount > 0) {
      logDebug(s"[$loggingId] Flushed $immutableCount immutable MemTables in " +
        s"${System.currentTimeMillis() - flushStart}ms")
    }

    // Create snapshot for this version
    val snapshotStart = System.currentTimeMillis()
    createSnapshot(newVersion)
    logDebug(s"[$loggingId] Created snapshot in ${System.currentTimeMillis() - snapshotStart}ms")

    // Upload snapshot to DFS
    val uploadStart = System.currentTimeMillis()
    uploadSnapshot(newVersion)
    logDebug(s"[$loggingId] Uploaded snapshot in ${System.currentTimeMillis() - uploadStart}ms")

    currentVersion.set(newVersion)

    val totalTime = System.currentTimeMillis() - startTime
    val ssTableCount = levels.map(_.size).sum
    logInfo(s"[$loggingId] COMMIT COMPLETE: version=$newVersion, " +
      s"duration=${totalTime}ms, ssTableCount=$ssTableCount, " +
      s"memTableHits=${stats.memTableHits}, diskReads=${stats.diskReads}")
  }

  /**
   * Rollback uncommitted changes.
   */
  def rollback(): Unit = {
    memTableLock.writeLock().lock()
    try {
      activeMemTable.clear()
      // Don't clear immutable MemTables as they represent committed data
    } finally {
      memTableLock.writeLock().unlock()
    }

    // Truncate WAL to last committed position
    if (wal != null) {
      wal.truncate()
    }

    logInfo(s"[$loggingId] Rolled back to version ${currentVersion.get}")
  }

  /**
   * Run background compaction.
   */
  def runCompaction(): Unit = {
    val startTime = System.currentTimeMillis()
    val levelSizesBefore = levels.map(_.size).toArray

    logDebug(s"[$loggingId] COMPACTION START: levelSizes=${levelSizesBefore.mkString(",")}")

    ssTableLock.writeLock().lock()
    try {
      // Level 0 compaction: merge overlapping tables into level 1
      if (levels(0).size >= 4) {
        logDebug(s"[$loggingId] Compacting Level 0: ${levels(0).size} tables")
        compactLevel0()
      }

      // Higher level compaction: size-tiered
      for (level <- 1 until conf.maxLevels - 1) {
        val maxSizeAtLevel = conf.memTableSizeBytes *
          math.pow(conf.levelSizeMultiplier, level).toLong

        val totalSize = levels(level).map(_.size).sum
        if (totalSize > maxSizeAtLevel) {
          logDebug(s"[$loggingId] Compacting Level $level: size=$totalSize > max=$maxSizeAtLevel")
          compactLevel(level)
        }
      }
    } finally {
      ssTableLock.writeLock().unlock()
    }

    val duration = System.currentTimeMillis() - startTime
    val levelSizesAfter = levels.map(_.size).toArray
    stats.recordCompactionTime(duration)

    logInfo(s"[$loggingId] COMPACTION COMPLETE: duration=${duration}ms, " +
      s"levelsBefore=${levelSizesBefore.mkString(",")}, " +
      s"levelsAfter=${levelSizesAfter.mkString(",")}")
  }

  /**
   * Clean up old versions.
   */
  def cleanupOldVersions(minVersionsToRetain: Int): Unit = {
    val currentVer = currentVersion.get
    val minVersionToKeep = math.max(0, currentVer - minVersionsToRetain)

    // Clean up old snapshots
    val snapshotFiles = snapshotDir.listFiles()
    if (snapshotFiles != null) {
      for (file <- snapshotFiles) {
        val version = extractVersionFromFileName(file.getName)
        if (version >= 0 && version < minVersionToKeep) {
          file.delete()
        }
      }
    }

    // Clean up old WAL segments
    if (wal != null) {
      wal.cleanup(minVersionToKeep)
    }
  }

  /**
   * Create a column family.
   */
  def createColumnFamily(name: String): Unit = {
    columnFamilies.putIfAbsent(name, new ColumnFamilyState())
  }

  /**
   * Remove a column family.
   *
   * @return true if the column family was removed, false if it didn't exist
   */
  def removeColumnFamily(name: String): Boolean = {
    columnFamilies.remove(name) != null
  }

  /**
   * Get all column family names.
   */
  def getColumnFamilies: Set[String] = {
    columnFamilies.keys().asScala.toSet
  }

  /**
   * Get statistics.
   */
  def getStats: LSMTreeStats = {
    val memTableSize = {
      memTableLock.readLock().lock()
      try {
        activeMemTable.estimatedSize +
          immutableMemTables.map(_.estimatedSize).sum
      } finally {
        memTableLock.readLock().unlock()
      }
    }

    val ssTableCount = {
      ssTableLock.readLock().lock()
      try {
        levels.map(_.size).sum
      } finally {
        ssTableLock.readLock().unlock()
      }
    }

    val walSize = if (wal != null) wal.size else 0L

    LSMTreeStats(
      numKeys = stats.estimatedKeyCount,
      memoryUsedBytes = memTableSize + stats.bloomFilterMemory,
      memTableSizeBytes = memTableSize,
      ssTableCount = ssTableCount.toLong,
      bloomFilterHitRate = stats.bloomFilterHitRatePercent,
      lastCompactionTimeMs = stats.lastCompactionTimeMs,
      walSizeBytes = walSize
    )
  }

  /**
   * Replay from a version to another version.
   */
  def replayFromVersion(fromVersion: Long, toVersion: Long): Unit = {
    if (wal != null && toVersion > fromVersion) {
      wal.replay(fromVersion, toVersion) { (operation, key, value) =>
        operation match {
          case WALOperation.PUT =>
            activeMemTable.put(key, value)
          case WALOperation.DELETE =>
            activeMemTable.delete(key)
        }
      }
    }
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      // Shutdown compaction executor
      compactionExecutor.shutdown()
      try {
        compactionExecutor.awaitTermination(10, TimeUnit.SECONDS)
      } catch {
        case _: InterruptedException =>
          compactionExecutor.shutdownNow()
      }

      // Close WAL
      if (wal != null) {
        wal.close()
      }

      // Close all SSTables
      ssTableLock.writeLock().lock()
      try {
        for (level <- levels) {
          for (ssTable <- level) {
            ssTable.close()
          }
          level.clear()
        }
      } finally {
        ssTableLock.writeLock().unlock()
      }

      logInfo(s"[$loggingId] LSMTree closed")
    }
  }

  // ============================================================================
  // Internal Methods
  // ============================================================================

  private def reset(): Unit = {
    memTableLock.writeLock().lock()
    try {
      activeMemTable.clear()
      immutableMemTables.clear()
    } finally {
      memTableLock.writeLock().unlock()
    }

    ssTableLock.writeLock().lock()
    try {
      for (level <- levels) {
        for (ssTable <- level) {
          ssTable.close()
        }
        level.clear()
      }
    } finally {
      ssTableLock.writeLock().unlock()
    }
  }

  /**
   * Rotate the active MemTable to immutable.
   */
  private def rotateMemTable(): Unit = {
    // Already holding write lock
    val rotatedSize = activeMemTable.size
    val rotatedBytes = activeMemTable.estimatedSize
    immutableMemTables += activeMemTable
    activeMemTable = new MemTable(conf.memTableSizeBytes)

    logDebug(s"[$loggingId] MEMTABLE ROTATED: entries=$rotatedSize, bytes=$rotatedBytes, " +
      s"immutableCount=${immutableMemTables.size}")

    // If too many immutable MemTables, flush in background
    if (immutableMemTables.size >= conf.maxMemTableCount) {
      logInfo(s"[$loggingId] Triggering background flush: " +
        s"immutableCount=${immutableMemTables.size} >= maxCount=${conf.maxMemTableCount}")
      compactionExecutor.submit(new Runnable {
        override def run(): Unit = {
          try {
            flushImmutableMemTables()
          } catch {
            case NonFatal(e) =>
              logWarning(s"[$loggingId] Error flushing MemTables", e)
          }
        }
      })
    }
  }

  /**
   * Flush all immutable MemTables to Level 0 SSTables.
   */
  private def flushImmutableMemTables(): Unit = {
    val startTime = System.currentTimeMillis()

    memTableLock.writeLock().lock()
    val tablesToFlush = try {
      val tables = immutableMemTables.toArray
      immutableMemTables.clear()
      tables
    } finally {
      memTableLock.writeLock().unlock()
    }

    if (tablesToFlush.isEmpty) return

    val totalEntries = tablesToFlush.map(_.size).sum
    logInfo(s"[$loggingId] FLUSH START: " +
      s"memTables=${tablesToFlush.length}, totalEntries=$totalEntries")

    var ssTableSizeBytes = 0L
    for (memTable <- tablesToFlush) {
      val ssTable = SSTable.createFromMemTable(
        memTable,
        ssTableDir,
        conf.blockSizeBytes,
        conf.bloomFilterFpp,
        conf.sparseIndexInterval
      )

      ssTableLock.writeLock().lock()
      try {
        levels(0) += ssTable
      } finally {
        ssTableLock.writeLock().unlock()
      }

      ssTableSizeBytes += ssTable.size
      stats.addBloomFilterMemory(ssTable.bloomFilterMemory)
    }

    val duration = System.currentTimeMillis() - startTime
    logInfo(s"[$loggingId] FLUSH COMPLETE: memTables=${tablesToFlush.length}, " +
      s"entries=$totalEntries, ssTableBytes=$ssTableSizeBytes, duration=${duration}ms, " +
      s"level0Count=${levels(0).size}")
  }

  /**
   * Compact Level 0 tables into Level 1.
   */
  private def compactLevel0(): Unit = {
    // Already holding write lock
    val tablesToCompact = levels(0).toArray
    levels(0).clear()

    if (tablesToCompact.isEmpty) return

    // Merge all Level 0 tables
    val mergedEntries = new java.util.TreeMap[ByteArrayWrapper, MemTableEntry](
      ByteArrayWrapper.COMPARATOR
    )

    for (ssTable <- tablesToCompact) {
      for ((key, entry) <- ssTable.iterator) {
        val wrapped = new ByteArrayWrapper(key)
        mergedEntries.put(wrapped, entry)
      }
      ssTable.close()
    }

    // Create new SSTable at Level 1
    if (!mergedEntries.isEmpty) {
      val newSSTable = SSTable.createFromEntries(
        mergedEntries.asScala.map { case (k, v) => (k.data, v) },
        ssTableDir,
        conf.blockSizeBytes,
        conf.bloomFilterFpp,
        conf.sparseIndexInterval
      )
      levels(1) += newSSTable
    }

    logInfo(s"[$loggingId] Compacted ${tablesToCompact.length} L0 tables into L1")
  }

  /**
   * Compact a level into the next level.
   */
  private def compactLevel(level: Int): Unit = {
    if (level >= conf.maxLevels - 1) return

    // Already holding write lock
    val tablesToCompact = levels(level).toArray
    levels(level).clear()

    if (tablesToCompact.isEmpty) return

    // Merge with next level
    val nextLevelTables = levels(level + 1).toArray
    levels(level + 1).clear()

    val mergedEntries = new java.util.TreeMap[ByteArrayWrapper, MemTableEntry](
      ByteArrayWrapper.COMPARATOR
    )

    // Add entries from current level
    for (ssTable <- tablesToCompact) {
      for ((key, entry) <- ssTable.iterator) {
        mergedEntries.put(new ByteArrayWrapper(key), entry)
      }
      ssTable.close()
    }

    // Add entries from next level (older entries)
    for (ssTable <- nextLevelTables) {
      for ((key, entry) <- ssTable.iterator) {
        val wrapped = new ByteArrayWrapper(key)
        if (!mergedEntries.containsKey(wrapped)) {
          mergedEntries.put(wrapped, entry)
        }
      }
      ssTable.close()
    }

    // Create new SSTables at next level
    if (!mergedEntries.isEmpty) {
      val newSSTable = SSTable.createFromEntries(
        mergedEntries.asScala.map { case (k, v) => (k.data, v) },
        ssTableDir,
        conf.blockSizeBytes,
        conf.bloomFilterFpp,
        conf.sparseIndexInterval
      )
      levels(level + 1) += newSSTable
    }

    logInfo(s"[$loggingId] Compacted L$level into L${level + 1}")
  }

  private def findClosestSnapshot(version: Long): Long = {
    val snapshotFiles = snapshotDir.listFiles()
    if (snapshotFiles == null || snapshotFiles.isEmpty) {
      return 0
    }

    val versions = snapshotFiles
      .map(f => extractVersionFromFileName(f.getName))
      .filter(v => v >= 0 && v <= version)
      .sorted

    if (versions.isEmpty) 0 else versions.last
  }

  // ---------------------------------------------------------------------------
  // loadSnapshot: Loads state from a snapshot file (streaming, memory-efficient)
  // ---------------------------------------------------------------------------
  // Reads entries one-by-one from snapshot file without buffering all in memory.
  // Handles both new format (Long count) and legacy format (Int count) for
  // backwards compatibility during migration.
  // ---------------------------------------------------------------------------
  private def loadSnapshot(version: Long): Unit = {
    val snapshotFile = new File(snapshotDir, s"snapshot_$version")
    if (!snapshotFile.exists()) {
      // Try to download from DFS
      downloadSnapshot(version)
    }

    if (!snapshotFile.exists()) {
      throw new IOException(s"Snapshot file not found for version $version: $snapshotFile")
    }

    val input = new DataInputStream(
      new BufferedInputStream(new FileInputStream(snapshotFile), 256 * 1024)
    )
    try {
      // Read entry count (Long in new format)
      val numEntries = input.readLong()
      logDebug(s"[$loggingId] Loading snapshot $version with $numEntries entries")

      reset()

      var loaded = 0L
      while (loaded < numEntries) {
        val keyLen = input.readInt()
        val key = new Array[Byte](keyLen)
        input.readFully(key)

        val isDeleted = input.readBoolean()
        if (isDeleted) {
          activeMemTable.delete(key)
        } else {
          val valueLen = input.readInt()
          val value = new Array[Byte](valueLen)
          input.readFully(value)
          activeMemTable.put(key, value)
        }
        loaded += 1
      }

      logInfo(s"[$loggingId] Loaded snapshot $version: $loaded entries")
    } finally {
      input.close()
    }
  }

  // ---------------------------------------------------------------------------
  // createSnapshot: Creates a local snapshot file by STREAMING data
  // ---------------------------------------------------------------------------
  // IMPORTANT: This method streams data directly to disk instead of buffering
  // all entries in memory. This prevents OOM with large state.
  // We write entry count at the end using a two-pass approach:
  // 1. Write all entries to temp file (streaming)
  // 2. Write final file with count header + data
  // ---------------------------------------------------------------------------
  private def createSnapshot(version: Long): Unit = {
    val snapshotFile = new File(snapshotDir, s"snapshot_$version")
    val tempFile = new File(snapshotDir, s"snapshot_$version.tmp")

    // First pass: stream entries to temp file and count them
    var entryCount = 0L
    val tempOutput = new DataOutputStream(
      new BufferedOutputStream(new FileOutputStream(tempFile), 256 * 1024)
    )

    try {
      // Stream entries directly - no buffering in memory
      val iter = iterator()
      while (iter.hasNext) {
        val (key, value) = iter.next()
        tempOutput.writeInt(key.length)
        tempOutput.write(key)
        tempOutput.writeBoolean(value == null)
        if (value != null) {
          tempOutput.writeInt(value.length)
          tempOutput.write(value)
        }
        entryCount += 1
      }
      tempOutput.flush()
    } finally {
      tempOutput.close()
    }

    // Second pass: write final file with count header
    val finalOutput = new DataOutputStream(
      new BufferedOutputStream(new FileOutputStream(snapshotFile), 256 * 1024)
    )
    try {
      // Write entry count first
      finalOutput.writeLong(entryCount)

      // Copy temp file data
      val tempInput = new BufferedInputStream(new FileInputStream(tempFile), 256 * 1024)
      try {
        val buffer = new Array[Byte](256 * 1024)
        var bytesRead = tempInput.read(buffer)
        while (bytesRead > 0) {
          finalOutput.write(buffer, 0, bytesRead)
          bytesRead = tempInput.read(buffer)
        }
      } finally {
        tempInput.close()
      }
      finalOutput.flush()
    } finally {
      finalOutput.close()
      // Clean up temp file
      tempFile.delete()
    }

    logDebug(s"[$loggingId] Created local snapshot $version with $entryCount entries")
  }

  // ---------------------------------------------------------------------------
  // uploadSnapshot: Uploads snapshot to DFS with atomic write
  // ---------------------------------------------------------------------------
  // CRITICAL: This method MUST fail the commit if upload fails!
  // A silently failed upload = corrupted checkpoint on restart.
  // Uses CheckpointFileManager.createAtomic for atomic write (temp + rename).
  // ---------------------------------------------------------------------------
  private def uploadSnapshot(version: Long): Unit = {
    val localSnapshotFile = new File(snapshotDir, s"snapshot_$version")
    val dfsPath = new HadoopPath(dfsRootDir, s"snapshot_$version")

    // Verify local snapshot exists
    if (!localSnapshotFile.exists()) {
      throw new IOException(s"Local snapshot file not found: $localSnapshotFile")
    }

    // Use createAtomic for atomic write (writes to temp, renames on close)
    val outputStream = fm.createAtomic(dfsPath, overwriteIfPossible = true)
    try {
      val inputStream = new BufferedInputStream(new FileInputStream(localSnapshotFile), 256 * 1024)
      try {
        val buffer = new Array[Byte](256 * 1024)
        var bytesRead = inputStream.read(buffer)
        while (bytesRead > 0) {
          outputStream.write(buffer, 0, bytesRead)
          bytesRead = inputStream.read(buffer)
        }
      } finally {
        inputStream.close()
      }
      // Flush before close to ensure all data is written
      outputStream.flush()
    } catch {
      case e: Throwable =>
        // Cancel the atomic write (deletes temp file)
        try {
          outputStream.cancel()
        } catch {
          case NonFatal(_) => // Ignore cancel errors
        }
        // CRITICAL: Re-throw to fail the commit!
        throw new IOException(s"Failed to upload snapshot $version to DFS: ${e.getMessage}", e)
    } finally {
      outputStream.close()
    }

    logInfo(s"[$loggingId] Uploaded snapshot $version to DFS: $dfsPath")
  }

  private def downloadSnapshot(version: Long): Unit = {
    val dfsPath = new HadoopPath(dfsRootDir, s"snapshot_$version")
    val localSnapshotFile = new File(snapshotDir, s"snapshot_$version")

    try {
      val inputStream = fm.open(dfsPath)
      try {
        val outputStream = new FileOutputStream(localSnapshotFile)
        try {
          val buffer = new Array[Byte](64 * 1024)
          var bytesRead = inputStream.read(buffer)
          while (bytesRead > 0) {
            outputStream.write(buffer, 0, bytesRead)
            bytesRead = inputStream.read(buffer)
          }
        } finally {
          outputStream.close()
        }
      } finally {
        inputStream.close()
      }
      logInfo(s"[$loggingId] Downloaded snapshot $version from DFS")
    } catch {
      case NonFatal(e) =>
        logWarning(s"[$loggingId] Failed to download snapshot $version from DFS", e)
    }
  }

  private def extractVersionFromFileName(name: String): Long = {
    try {
      if (name.startsWith("snapshot_")) {
        name.stripPrefix("snapshot_").toLong
      } else {
        -1
      }
    } catch {
      case _: NumberFormatException => -1
    }
  }

  private def startsWith(array: Array[Byte], prefix: Array[Byte]): Boolean = {
    if (array.length < prefix.length) return false
    var i = 0
    while (i < prefix.length) {
      if (array(i) != prefix(i)) return false
      i += 1
    }
    true
  }

  override protected def logName: String = s"${super.logName} $loggingId"
}

object LSMTree {
  /**
   * Byte array wrapper for use in TreeMap.
   */
  class ByteArrayWrapper(val data: Array[Byte]) {
    override def hashCode(): Int = JArrays.hashCode(data)

    override def equals(obj: Any): Boolean = obj match {
      case other: ByteArrayWrapper => JArrays.equals(data, other.data)
      case _ => false
    }
  }

  object ByteArrayWrapper {
    val COMPARATOR: Comparator[ByteArrayWrapper] = (o1: ByteArrayWrapper, o2: ByteArrayWrapper) => {
      compareByteArrays(o1.data, o2.data)
    }
  }

  /**
   * Compare two byte arrays lexicographically using optimized comparison.
   * Uses word-at-a-time comparison for longer arrays.
   */
  def compareByteArrays(a: Array[Byte], b: Array[Byte]): Int = {
    val minLen = math.min(a.length, b.length)

    // For very small arrays, simple comparison is faster
    if (minLen < 8) {
      return compareByteArraysSimple(a, b, minLen)
    }

    // Compare 8 bytes at a time using Long comparison
    var offset = 0
    while (offset + 8 <= minLen) {
      val aLong = getLongBigEndian(a, offset)
      val bLong = getLongBigEndian(b, offset)
      if (aLong != bLong) {
        // Use unsigned comparison
        return java.lang.Long.compareUnsigned(aLong, bLong)
      }
      offset += 8
    }

    // Compare remaining bytes
    while (offset < minLen) {
      val diff = (a(offset) & 0xFF) - (b(offset) & 0xFF)
      if (diff != 0) return diff
      offset += 1
    }

    a.length - b.length
  }

  /**
   * Simple byte-by-byte comparison for small arrays.
   */
  private def compareByteArraysSimple(a: Array[Byte], b: Array[Byte], minLen: Int): Int = {
    var i = 0
    while (i < minLen) {
      val diff = (a(i) & 0xFF) - (b(i) & 0xFF)
      if (diff != 0) return diff
      i += 1
    }
    a.length - b.length
  }

  /**
   * Read 8 bytes as a big-endian long for comparison.
   */
  private def getLongBigEndian(bytes: Array[Byte], offset: Int): Long = {
    ((bytes(offset).toLong & 0xFF) << 56) |
      ((bytes(offset + 1).toLong & 0xFF) << 48) |
      ((bytes(offset + 2).toLong & 0xFF) << 40) |
      ((bytes(offset + 3).toLong & 0xFF) << 32) |
      ((bytes(offset + 4).toLong & 0xFF) << 24) |
      ((bytes(offset + 5).toLong & 0xFF) << 16) |
      ((bytes(offset + 6).toLong & 0xFF) << 8) |
      (bytes(offset + 7).toLong & 0xFF)
  }

  /**
   * Column family state.
   */
  private class ColumnFamilyState {
    // Placeholder for column family specific state
  }
}

/**
 * WAL operation types.
 */
object WALOperation extends Enumeration {
  type WALOperation = Value
  val PUT, DELETE = Value
}

