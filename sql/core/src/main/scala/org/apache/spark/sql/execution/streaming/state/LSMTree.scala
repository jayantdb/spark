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
 * =======================================================================
 * LSM-TREE (Log-Structured Merge-Tree) - High-Performance Key-Value Store
 * =======================================================================
 *
 * WHAT IS AN LSM-TREE
 * ------------------
 * It's a 2-tier filing system:
 * 1. MemTable: Small, in-memory, for recent writes
 * 2. SSTables: Large, on-disk, for archived data
 *
 * HOW WRITES WORK (Super Fast)
 * ---------------------------
 * 1. Write to WAL (backup, crash safe)
 * 2. Write to MemTable (in-memory ConcurrentSkipListMap)
 * 3. When MemTable is full, flush to SSTable on disk
 *
 * HOW READS WORK
 * -------------
 * 1. Check MemTable first (newest data, in memory)
 * 2. Check SSTables (oldest to newest, use Bloom filters to skip)
 * Bloom filters tell us "definitely not here" or "maybe here".
 *
 * WHY COMPACTION
 * -------------
 * Over time, we accumulate many SSTables with duplicate/deleted keys.
 * Compaction merges them into fewer.
 * cleaner files = faster reads
 *
 * Key features:
 * - Pure Scala/JVM implementation (no JNI overhead)
 * - Thread-safe with fine-grained locking
 * - Memory-efficient streaming I/O (no OOM with large state)
 * - Bloom filters for read optimization (skip SSTables that don't have key)
 * - Write-Ahead Log for durability (survive crashes)
 * - Tiered compaction for handling large state
 *
 * @param localRootDir Local directory for working files (SSTables, WAL)
 * @param dfsRootDir DFS directory for checkpointing (HDFS/S3/etc)
 * @param conf LSM-Tree configuration (memtable size, bloom filter FPP, etc)
 * @param fm Checkpoint file manager (handles atomic writes to DFS)
 * @param hadoopConf Hadoop configuration
 * @param loggingId Identifier for logging (usually partition info)
 */
@ThreadSafe
class LSMTree(
    localRootDir: File,         // Where to store local files (fast SSD preferred)
    dfsRootDir: String,         // Where to upload snapshots (HDFS/S3 for durability)
    conf: LSMTreeConf,          // Tuning parameters (memtable size, bloom, etc)
    fm: CheckpointFileManager,  // Handles atomic file operations on DFS
    hadoopConf: Configuration,  // Hadoop config for DFS access
    loggingId: String)          // For log messages (e.g., "partition=5")
    extends Logging with Closeable {

  import LSMTree._

  // DIRECTORY STRUCTURE - Where we store different types of data
  // Not actively used, but reserved for future memtable persistence
  private val memTableDir = new File(localRootDir, "memtable")

  // Sorted String Tables live here - immutable, sorted key-value files
  private val ssTableDir = new File(localRootDir, "sstables")

  // Write-Ahead Log files - sequential log of all writes for crash recovery
  private val walDir = new File(localRootDir, "wal")

  // Snapshot files - full state dumps at specific versions for fast recovery
  private val snapshotDir = new File(localRootDir, "snapshots")

  // Create all directories if they don't exist
  Seq(memTableDir, ssTableDir, walDir, snapshotDir).foreach { dir =>
    if (!dir.exists()) {
      dir.mkdirs()
    }
  }

  // CORE COMPONENTS

  // Lock for memtable operations - ReadWriteLock allows concurrent reads
  private val memTableLock = new ReentrantReadWriteLock()
  private val ssTableLock = new ReentrantReadWriteLock() // Lock for SSTable operations

  // ACTIVE MEMTABLE: Current in-memory buffer for writes
  // Uses ConcurrentSkipListMap for thread-safe sorted access
  // When it fills up, it becomes "immutable" and a new one takes over
  @GuardedBy("memTableLock")
  private var activeMemTable: MemTable = new MemTable(conf.memTableSizeBytes)

  // IMMUTABLE MEMTABLES: Full memtables waiting to be flushed to disk
  // We keep writes going to a new memtable while these flush in background
  @GuardedBy("memTableLock")
  private val immutableMemTables: mutable.ArrayBuffer[MemTable] = mutable.ArrayBuffer.empty

  // SSTABLE LEVELS: Disk-based sorted files organized by "age"
  // Level 0 = freshest (just flushed from memtable), may have overlapping keys
  // Level 1+ = older, compacted, non-overlapping within level
  // Higher levels = larger, older data
  @GuardedBy("ssTableLock")
  private val levels: Array[mutable.ArrayBuffer[SSTable]] =
    Array.fill(conf.maxLevels)(mutable.ArrayBuffer.empty)

  // WAL: Sequential log of all writes for crash recovery
  // Every put/delete goes here first, then to memtable
  // On crash, we replay WAL to recover uncommitted writes
  private val wal: WriteAheadLog = if (conf.walEnabled) {
    new WriteAheadLog(walDir, loggingId)
  } else {
    null  // WAL can be disabled for testing (not recommended in production)
  }

  // COLUMN FAMILIES: Logical partitions within the state store
  // Like separate "tables" that share the same LSM-Tree structure
  // Currently simple - each family just has a state marker
  private val columnFamilies: ConcurrentHashMap[String, ColumnFamilyState] =
    new ConcurrentHashMap[String, ColumnFamilyState]()

  // Every LSM-Tree starts with a default column family
  columnFamilies.put(StateStore.DEFAULT_COL_FAMILY_NAME, new ColumnFamilyState())

  // CURRENT VERSION: Tracks the latest committed version
  // Increments with each successful commit (like a transaction counter)
  private val currentVersion = new AtomicLong(0L)

  // BACKGROUND COMPACTION: Thread pool for merging SSTables
  // Compaction runs async to avoid blocking writes
  private val compactionExecutor: ScheduledExecutorService =
    Executors.newScheduledThreadPool(Math.min(conf.compactionThreads, 2))

  // STATISTICS: Tracks hits, misses, read/write counts for monitoring
  private val stats = new LSMTreeStatsCollector()

  // SHUTDOWN FLAG: Prevents double-close and ensures clean shutdown
  private val closed = new AtomicBoolean(false)

  // LOAD - Restore state to a specific version (called on partition startup)

  // Recovery strategy:
  // 1. Find the closest snapshot <= target version
  // 2. Load that snapshot (bulk load of all key-values)
  // 3. Replay WAL entries from snapshot version to target version
  // This is faster than replaying ALL WAL entries from the beginning!
  def load(version: Long): Unit = {
    val startTime = System.currentTimeMillis()
    logInfo(s"[$loggingId] LOAD START: targetVersion=$version")

    // Versions are always non-negative
    if (version < 0) {
      throw new IllegalArgumentException(s"Version must be non-negative: $version")
    }

    // Version 0 = brand new state, nothing to load
    if (version == 0) {
      logDebug(s"[$loggingId] Fresh start - resetting state")
      reset()  // Clear any existing state
      currentVersion.set(0)
      logInfo(s"[$loggingId] LOAD COMPLETE: version=0 (fresh start), duration=0ms")
      return
    }

    // Step 1: Find the best snapshot to start from
    // Example: target=100, snapshots exist at [10, 50, 80] -> use 80
    val snapshotVersion = findClosestSnapshot(version)
    logDebug(s"[$loggingId] Found closest snapshot: version=$snapshotVersion")

    // Step 2: Load snapshot (or start fresh if no snapshot exists)
    if (snapshotVersion > 0) {
      val snapshotStart = System.currentTimeMillis()
      loadSnapshot(snapshotVersion)  // Bulk load all key-values from snapshot file
      logDebug(s"[$loggingId] Loaded snapshot $snapshotVersion in " +
        s"${System.currentTimeMillis() - snapshotStart}ms")
    } else {
      logDebug(s"[$loggingId] No snapshot found, starting fresh")
      reset()
    }

    // Step 3: Replay WAL to catch up the range from snapshot to target version
    // Example: snapshot=80, target=100 -> replay WAL entries 81-100
    if (wal != null && version > snapshotVersion) {
      val walStart = System.currentTimeMillis()
      var walEntryCount = 0L
      // Replay each WAL entry by applying it to the memtable
      wal.replay(snapshotVersion, version) { (operation, key, value) =>
        operation match {
          case WALOperation.PUT => activeMemTable.put(key, value)
          case WALOperation.DELETE => activeMemTable.delete(key)
        }
        walEntryCount += 1
      }
      logDebug(s"[$loggingId] Replayed $walEntryCount WAL entries from version $snapshotVersion " +
        s"to $version in ${System.currentTimeMillis() - walStart}ms")
    }

    // Update current version to reflect loaded state
    currentVersion.set(version)
    val totalTime = System.currentTimeMillis() - startTime
    val keyCount = activeMemTable.size + immutableMemTables.map(_.size).sum
    logInfo(s"[$loggingId] LOAD COMPLETE: version=$version, snapshotVersion=$snapshotVersion, " +
      s"keyCount=$keyCount, duration=${totalTime}ms")
  }

  // GET - Read a value by key

  // Read path priority (newest data first):
  // 1. Active MemTable - newest writes, in memory, super fast
  // 2. Immutable MemTables - slightly older, still in memory
  // 3. SSTables - oldest data, on disk, use Bloom filters to skip
  //
  // Newer writes override older ones, so we check newest first.
  // If we find a tombstone (delete marker), we return null immediately.
  def get(
           key: Array[Byte],
           colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME
         ): Array[Byte] = {
    stats.incrementReads()

    // Step 1: Check active MemTable (lock-free ConcurrentSkipListMap read)
    val activeResult = activeMemTable.get(key)
    if (activeResult != null) {
      // Found the key. Check if it's a tombstone (deleted key)
      if (activeResult.isDeleted) return null
      stats.incrementMemTableHits()
      return activeResult.value
    }

    // Step 2: Check immutable MemTables (newest first = reverse order)
    // Need read lock since these can be modified during flush
    memTableLock.readLock().lock()
    try {
      for (memTable <- immutableMemTables.reverseIterator) {
        val immResult = memTable.get(key)
        if (immResult != null) {
          if (immResult.isDeleted) return null  // Tombstone = key was deleted
          stats.incrementMemTableHits()
          return immResult.value
        }
      }
    } finally {
      memTableLock.readLock().unlock()
    }

    // Step 3: Check SSTables on disk (newest first, level by level)
    // This is the slowest path, but Bloom filters help us skip most SSTables!
    ssTableLock.readLock().lock()
    try {
      for (level <- 0 until conf.maxLevels) {
        for (ssTable <- levels(level).reverseIterator) {
          // bloom filter logic: Quickly check if key might be in this SSTable
          if (ssTable.mightContain(key)) {
            stats.incrementBloomFilterChecks()
            // if bloom indicates the key maybe here, do actual disk lookup
            val result = ssTable.get(key)
            if (result != null) {
              if (result.isDeleted) return null
              stats.incrementDiskReads()
              return result.value
            }
          } else {
            // if bloom indicates the key is not here, skip the SSTable.
            stats.incrementBloomFilterSkips()
          }
        }
      }
    } finally {
      ssTableLock.readLock().unlock()
    }
    null // return null if key not found anywhere
  }

  // PUT - Write a key-value pair

  // Write path (designed for speed):
  // 1. Append to WAL (sequential disk write - fast!)
  // 2. Insert into MemTable (in-memory tree - instant!)
  // 3. If MemTable is full, rotate it (background flush will handle it)
  //
  // This is just a sequential log + memory.
  def put(
           key: Array[Byte],
           value: Array[Byte],
           colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME
         ): Unit = {
    stats.incrementWrites()

    // Step 1: Write to WAL FIRST (durability before visibility)
    // If we crash after this, we can recover by replaying the WAL
    if (wal != null) {
      wal.append(WALOperation.PUT, key, value)
    }

    // Step 2: Write to MemTable (now visible to reads)
    memTableLock.writeLock().lock()
    try {
      activeMemTable.put(key, value)

      // Step 3: Check if MemTable is full and needs rotation
      // Rotation = current memtable becomes immutable, new one created
      if (activeMemTable.estimatedSize >= conf.memTableSizeBytes) {
        rotateMemTable()
      }
    } finally {
      memTableLock.writeLock().unlock()
    }
  }

  // DELETE - Remove a key (actually writes a "tombstone" marker!)

  // In LSM-Trees, we don't actually delete data immediately.
  // Because the key might exist in older SSTables we can't easily modify.
  // Instead, we write a "tombstone" (delete marker or soft delete)
  // During reads, if we see a tombstone, we return null.
  // During compaction, tombstones and their old values get cleaned up.
  def delete(
      key: Array[Byte],
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit = {
    stats.incrementWrites()

    // Step 1: Write tombstone to WAL (value=null indicates delete)
    if (wal != null) {
      wal.append(WALOperation.DELETE, key, null)
    }

    // Step 2: Write tombstone to MemTable
    memTableLock.writeLock().lock()
    try {
      activeMemTable.delete(key)  // Inserts a tombstone entry

      // Check if MemTable needs rotation
      if (activeMemTable.estimatedSize >= conf.memTableSizeBytes) {
        rotateMemTable()
      }
    } finally {
      memTableLock.writeLock().unlock()
    }
  }

  // ITERATOR - Scan all key-value pairs (used for full state iteration)

  // This is a "merged" iterator that combines data from all sources like
  // Active MemTable, Immutable MemTables, and SSTables
  // The MergingIterator handles deduplication (newest version wins) and
  // filters out tombstones (deleted keys).
  def iterator(
                colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME
              ): Iterator[(Array[Byte], Array[Byte])] = {
    // Collect iterators from all data sources
    val iterators = new ArrayBuffer[Iterator[(Array[Byte], MemTableEntry)]]()

    memTableLock.readLock().lock()
    try {
      // Add active MemTable iterator (clone keys to avoid mutation issues)
      iterators += activeMemTable.iterator.map { case (k, v) => (k.clone(), v) }

      // Add immutable MemTable iterators (newest first)
      for (memTable <- immutableMemTables.reverseIterator) {
        iterators += memTable.iterator.map { case (k, v) => (k.clone(), v) }
      }
    } finally {
      memTableLock.readLock().unlock()
    }

    ssTableLock.readLock().lock()
    try {
      // Add SSTable iterators from all levels
      for (level <- 0 until conf.maxLevels) {
        for (ssTable <- levels(level).reverseIterator) {
          iterators += ssTable.iterator
        }
      }
    } finally {
      ssTableLock.readLock().unlock()
    }

    // MergingIterator: merges sorted iterators, deduplicates, filters tombstones
    new MergingIterator(iterators.toSeq)
  }

  // PREFIX SCAN - Efficiently find all keys starting with a prefix

  // Example: prefix="user_123_" finds "user_123_name", "user_123_email", etc.
  //
  // Uses SkipListMap.tailMap() and SSTable sparse index to seek directly to the prefix start
  // Avoids a full scan and makes scan faster
  def prefixScan(
                  prefix: Array[Byte],
                  colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME
                ): Iterator[(Array[Byte], Array[Byte])] = {
    // Collect prefix-filtered iterators from all sources
    val iterators = new ArrayBuffer[Iterator[(Array[Byte], MemTableEntry)]]()

    memTableLock.readLock().lock()
    try {
      // Active MemTable - uses tailMap to seek to prefix start
      iterators += activeMemTable.prefixIterator(prefix).map { case (k, v) => (k.clone(), v) }

      // Immutable MemTables
      for (memTable <- immutableMemTables.reverseIterator) {
        iterators += memTable.prefixIterator(prefix).map { case (k, v) => (k.clone(), v) }
      }
    } finally {
      memTableLock.readLock().unlock()
    }

    ssTableLock.readLock().lock()
    try {
      // SSTables - use sparse index to seek near prefix start
      for (level <- 0 until conf.maxLevels) {
        for (ssTable <- levels(level).reverseIterator) {
          iterators += ssTable.prefixIterator(prefix)
        }
      }
    } finally {
      ssTableLock.readLock().unlock()
    }

    // Merge and deduplicate results
    new MergingIterator(iterators.toSeq)
  }

  // RANGE SCAN - Find all keys in range [startKey, endKey)

  // The range is inclusive on start, exclusive on end (like Python slicing).
  // Example: rangeScan("a", "d") returns keys "a", "abc", "b", "c" but not "d".
  // Uses seeking to avoid scanning from the beginning of the data.
  def rangeScan(
                 startKey: Array[Byte],
                 endKey: Array[Byte],
                 colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME
               ): Iterator[(Array[Byte], Array[Byte])] = {
    // Collect range iterators from all sources
    val iterators = new ArrayBuffer[Iterator[(Array[Byte], MemTableEntry)]]()

    memTableLock.readLock().lock()
    try {
      // Active MemTable - use subMap for range
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
      // SSTables - seek to startKey, stop at endKey
      for (level <- 0 until conf.maxLevels) {
        for (ssTable <- levels(level).reverseIterator) {
          // Seek to startKey, then take entries while key < endKey
          iterators += ssTable.iteratorFrom(startKey).takeWhile { case (key, _) =>
            compareByteArrays(key, endKey) < 0
          }
        }
      }
    } finally {
      ssTableLock.readLock().unlock()
    }

    // Merge and deduplicate results
    new MergingIterator(iterators.toSeq)
  }

  // COMMIT - Persist all changes and make them durable (called per micro-batch)

  // Commit steps:
  // 1. Sync WAL to disk (ensures writes survive crash)
  // 2. Rotate active MemTable to immutable (frees it for flush)
  // 3. Flush immutable MemTables to SSTables (persist to disk)
  // 4. Create local snapshot (full state dump for fast recovery)
  // 5. Upload snapshot to DFS (for distributed recovery)
  //
  // If any step fails, the commit fails. No partial commits.
  def commit(newVersion: Long): Unit = {
    val startTime = System.currentTimeMillis()
    val memTableSize = activeMemTable.size
    val memTableBytes = activeMemTable.estimatedSize

    logInfo(s"[$loggingId] COMMIT START: version=$newVersion, " +
      s"pendingKeys=$memTableSize, memTableBytes=$memTableBytes")

    // Step 1: Sync WAL - force all buffered WAL writes to disk
    // This ensures we can recover uncommitted writes after a crash
    if (wal != null) {
      val walStart = System.currentTimeMillis()
      wal.sync()
      logDebug(s"[$loggingId] WAL synced in ${System.currentTimeMillis() - walStart}ms")
    }

    // Step 2: Rotate MemTable if it has data
    // Moves active memtable to immutable MemTables, creates fresh active memtable
    memTableLock.writeLock().lock()
    try {
      if (activeMemTable.size > 0) {
        logDebug(s"[$loggingId] Rotating MemTable with ${activeMemTable.size} entries")
        rotateMemTable()
      }
    } finally {
      memTableLock.writeLock().unlock()
    }

    // Step 3: Flush all immutable MemTables to SSTables on disk
    // This converts in-memory data to persistent sorted files
    val flushStart = System.currentTimeMillis()
    val immutableCount = immutableMemTables.size
    flushImmutableMemTables()
    if (immutableCount > 0) {
      logDebug(s"[$loggingId] Flushed $immutableCount immutable MemTables in " +
        s"${System.currentTimeMillis() - flushStart}ms")
    }

    // Step 4: Create local snapshot for this version
    // Snapshot = full dump of all state for fast recovery (no WAL replay needed)
    val snapshotStart = System.currentTimeMillis()
    createSnapshot(newVersion)
    logDebug(s"[$loggingId] Created snapshot in ${System.currentTimeMillis() - snapshotStart}ms")

    // Step 5: Upload snapshot to DFS for distributed durability
    // This must succeed otherwise commit fails
    val uploadStart = System.currentTimeMillis()
    uploadSnapshot(newVersion)
    logDebug(s"[$loggingId] Uploaded snapshot in ${System.currentTimeMillis() - uploadStart}ms")

    // Update current version to reflect successful commit
    currentVersion.set(newVersion)

    val totalTime = System.currentTimeMillis() - startTime
    val ssTableCount = levels.map(_.size).sum
    logInfo(s"[$loggingId] COMMIT COMPLETE: version=$newVersion, " +
      s"duration=${totalTime}ms, ssTableCount=$ssTableCount, " +
      s"memTableHits=${stats.memTableHits}, diskReads=${stats.diskReads}")
  }

  // ROLLBACK - Discard uncommitted changes (called on failure/abort)

  // Simply clears the active MemTable and truncates WAL.
  // Immutable MemTables are NOT cleared as they represent already-committed data.
  // After rollback, the state is exactly as it was at the last commit
  def rollback(): Unit = {
    memTableLock.writeLock().lock()
    try {
      activeMemTable.clear()
      // ImmutableMemTables have committed data, don't touch them.
    } finally {
      memTableLock.writeLock().unlock()
    }

    // Truncate WAL - remove entries after last commit
    if (wal != null) {
      wal.truncate()
    }

    logInfo(s"[$loggingId] Rolled back to version ${currentVersion.get}")
  }

  // COMPACTION - Merge SSTables to improve read performance

  // Why compaction?
  // - Over time, we accumulate many SSTables with overlapping key ranges
  // - Reads must check all SSTables which might become slower as we grow
  // - Deleted keys (tombstones) still take space until compaction
  //
  // Compaction strategy:
  // - Level 0: Merge all L0 tables into L1 when L0 has 4+ tables
  // - Level 1+: Merge into next level when size exceeds threshold
  //
  // This is called in background to avoid blocking writes
  def runCompaction(): Unit = {
    val startTime = System.currentTimeMillis()
    val levelSizesBefore = levels.map(_.size)

    logDebug(s"[$loggingId] COMPACTION START: levelSizes=${levelSizesBefore.mkString(",")}")

    ssTableLock.writeLock().lock()
    try {
      // Level 0 compaction: L0 tables may overlap, so merge all together
      // Trigger when we have 4+ L0 tables.
      // Jayant Todo: Currently I am keeping 4 fixed. I will make it a spark config later.
      if (levels(0).size >= 4) {
        logDebug(s"[$loggingId] Compacting Level 0: ${levels(0).size} tables")
        compactLevel0()
      }

      // We determines how much larger each level is compared to the previous level.
      // Each level has a max size = memTableSize * (multiplier ^ level)
      for (level <- 1 until conf.maxLevels - 1) {
        val maxSizeAtLevel =
          conf.memTableSizeBytes * math.pow(conf.levelSizeMultiplier, level).toLong

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
    val levelSizesAfter = levels.map(_.size)

    stats.recordCompactionTime(duration)

    logInfo(s"[$loggingId] COMPACTION COMPLETE: duration=${duration}ms, " +
      s"levelsBefore=${levelSizesBefore.mkString(",")}, " +
      s"levelsAfter=${levelSizesAfter.mkString(",")}")
  }

  // CLEANUP - Delete old snapshots and WAL segments to free disk space

  // Spark keeps multiple versions for failure recovery.
  // This method deletes versions older than (current - minVersionsToRetain).
  // Called periodically by maintenance tasks
  def cleanupOldVersions(minVersionsToRetain: Int): Unit = {
    val currentVer = currentVersion.get
    // Calculate the oldest version we need to keep
    val minVersionToKeep = math.max(0, currentVer - minVersionsToRetain)

    // Delete old snapshot files
    val snapshotFiles = snapshotDir.listFiles()
    if (snapshotFiles != null) {
      for (file <- snapshotFiles) {
        val version = extractVersionFromFileName(file.getName)
        if (version >= 0 && version < minVersionToKeep) {
          file.delete()
        }
      }
    }

    // Delete old WAL segments
    if (wal != null) {
      wal.cleanup(minVersionToKeep)
    }
  }

  // COLUMN FAMILY MANAGEMENT - Logical partitions within the state store

  // Column families are like separate tables that share infrastructure.
  // Example: "users" family for user state, "sessions" family for session state.
  // Each family can have different key-value schemas

  // Create a new column family (no-op if already exists)
  def createColumnFamily(name: String): Unit = {
    columnFamilies.putIfAbsent(name, new ColumnFamilyState())
  }

  // Remove a column family and its state
  // Returns true if removed, false if it didn't exist
  def removeColumnFamily(name: String): Boolean = {
    columnFamilies.remove(name) != null
  }

  // Get all column family names (for iteration/debugging)
  def getColumnFamilies: Set[String] = {
    columnFamilies.keys().asScala.toSet
  }

  // STATISTICS - Performance metrics for monitoring and debugging

  // Returns current stats like memory usage, SSTable count, Bloom filter hit rate.
  // This is useful for:
  // - Monitoring dashboard (is state growing too large?)
  // - Performance tuning (are Bloom filters effective?)
  // - Debugging (why are reads slow?)
  def getStats: LSMTreeStats = {
    // Calculate total memory used by MemTables
    val memTableSize = {
      memTableLock.readLock().lock()
      try {
        activeMemTable.estimatedSize + immutableMemTables.map(_.estimatedSize).sum
      } finally {
        memTableLock.readLock().unlock()
      }
    }

    // Count total SSTables across all levels
    val ssTableCount = {
      ssTableLock.readLock().lock()
      try {
        levels.map(_.size).sum
      } finally {
        ssTableLock.readLock().unlock()
      }
    }

    // Get WAL size (0 if WAL is disabled)
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

  // REPLAY - Apply WAL entries to recover state (used during load)

  // Replays all operations between two versions to the active MemTable.
  // This is to catch up from a snapshot to a target version
  def replayFromVersion(fromVersion: Long, toVersion: Long): Unit = {
    if (wal != null && toVersion > fromVersion) {
      // Iterate through WAL entries and apply each operation
      wal.replay(fromVersion, toVersion) { (operation, key, value) =>
        operation match {
          case WALOperation.PUT => activeMemTable.put(key, value)
          case WALOperation.DELETE => activeMemTable.delete(key)
        }
      }
    }
  }

  // CLOSE - Clean shutdown of the LSM-Tree

  // Properly releases all resources.
  // 1. Stops background compaction threads
  // 2. Closes WAL (flushes any buffered writes)
  // 3. Closes all SSTable file handles
  override def close(): Unit = {
    // Use compareAndSet to ensure we only close once
    if (closed.compareAndSet(false, true)) {
      // Step 1: Shutdown compaction thread pool gracefully
      compactionExecutor.shutdown()
      try {
        compactionExecutor.awaitTermination(10, TimeUnit.SECONDS)
      } catch {
        case _: InterruptedException =>
          compactionExecutor.shutdownNow()  // Force shutdown if interrupted
      }

      // Step 2: Close WAL (flushes and closes file)
      if (wal != null) {
        wal.close()
      }

      // Step 3: Close all SSTables (releases file handles)
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

  // INTERNAL METHODS - Implementation details (not part of public API)

  // reset: Clear all in-memory and disk state (for fresh start or testing)
  private def reset(): Unit = {
    // Clear all MemTables
    memTableLock.writeLock().lock()
    try {
      activeMemTable.clear()
      immutableMemTables.clear()
    } finally {
      memTableLock.writeLock().unlock()
    }

    // Close and clear all SSTables
    ssTableLock.writeLock().lock()
    try {
      for (level <- levels) {
        for (ssTable <- level) {
          ssTable.close()  // Release file handles
        }
        level.clear()
      }
    } finally {
      ssTableLock.writeLock().unlock()
    }
  }

  // rotateMemTable: Move active MemTable to immutable queue, create new one

  // Called when active MemTable is full. The old one joins the "immutable" queue
  // waiting to be flushed to disk. A fresh MemTable takes over for new writes.
  // This ensures writes are never blocked by slow disk I/O.
  private def rotateMemTable(): Unit = {
    // Note: Caller must already hold memTableLock.writeLock()
    val rotatedSize = activeMemTable.size
    val rotatedBytes = activeMemTable.estimatedSize

    // Move current memtable to immutable queue
    immutableMemTables += activeMemTable

    // Create fresh active memtable
    activeMemTable = new MemTable(conf.memTableSizeBytes)

    logDebug(s"[$loggingId] MEMTABLE ROTATED: entries=$rotatedSize, bytes=$rotatedBytes, " +
      s"immutableCount=${immutableMemTables.size}")

    // If too many immutable MemTables queued, trigger background flush
    // This avoids memory from growing unbounded
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

  // flushImmutableMemTables: Convert MemTables to SSTables (memory -> disk)

  // This is where data becomes persistent. Each immutable MemTable becomes a new Level 0 SSTable.
  // SSTables are:
  // - Sorted by key (for efficient lookups)
  // - Immutable (never modified after creation)
  // - Have Bloom filters (for fast "not found" checks)
  private def flushImmutableMemTables(): Unit = {
    val startTime = System.currentTimeMillis()

    // Grab all immutable memtables to flush
    memTableLock.writeLock().lock()
    val tablesToFlush = try {
      val tables = immutableMemTables.toArray
      immutableMemTables.clear()  // Clear the queue
      tables
    } finally {
      memTableLock.writeLock().unlock()
    }

    if (tablesToFlush.isEmpty) return

    val totalEntries = tablesToFlush.map(_.size).sum
    logInfo(s"[$loggingId] FLUSH START: " +
      s"memTables=${tablesToFlush.length}, totalEntries=$totalEntries")

    // Convert each MemTable to an SSTable
    var ssTableSizeBytes = 0L
    for (memTable <- tablesToFlush) {
      // Create SSTable with Bloom filter and sparse index
      val ssTable = SSTable.createFromMemTable(
        memTable,
        ssTableDir,
        conf.blockSizeBytes,
        conf.bloomFilterFpp,       // Bloom filter false positive probability
        conf.sparseIndexInterval   // How often to add index entries
      )

      // Add to Level 0 (newest SSTables)
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

  // compactLevel0: Merge all Level 0 SSTables into a single Level 1 SSTable

  // SSTables can have overlapping key ranges (because they come from separate MemTable flushes).
  // This makes reads slower because we must check all L0 tables.
  // Compaction merges them into non-overlapping L1
  private def compactLevel0(): Unit = {
    // Note: Caller must already hold ssTableLock.writeLock()
    val tablesToCompact = levels(0).toArray
    levels(0).clear()

    if (tablesToCompact.isEmpty) return

    // Merge all L0 tables into a TreeMap (handles duplicates by keeping newest)
    val mergedEntries = new java.util.TreeMap[ByteArrayWrapper, MemTableEntry](
      ByteArrayWrapper.COMPARATOR
    )

    for (ssTable <- tablesToCompact) {
      for ((key, entry) <- ssTable.iterator) {
        val wrapped = new ByteArrayWrapper(key)
        mergedEntries.put(wrapped, entry)  // Later puts override earlier ones
      }
      ssTable.close()  // Release file handle
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

  // compactLevel: Merge a level into the next level (size-tiered compaction)

  // When a level exceeds its size threshold, we merge it with the next level.
  // This keeps the LSM-Tree from having too many levels (which slows reads).
  // Newer entries override older ones during merge
  private def compactLevel(level: Int): Unit = {
    // Can't compact beyond the last level
    if (level >= conf.maxLevels - 1) return

    // Note: Caller must already hold ssTableLock.writeLock()
    val tablesToCompact = levels(level).toArray
    levels(level).clear()

    if (tablesToCompact.isEmpty) return

    // Also grab next level's tables (we're merging into them)
    val nextLevelTables = levels(level + 1).toArray
    levels(level + 1).clear()

    // Merge all entries into TreeMap
    val mergedEntries = new java.util.TreeMap[ByteArrayWrapper, MemTableEntry](
      ByteArrayWrapper.COMPARATOR
    )

    // Add current level entries first (newer = higher priority)
    for (ssTable <- tablesToCompact) {
      for ((key, entry) <- ssTable.iterator) {
        mergedEntries.put(new ByteArrayWrapper(key), entry)
      }
      ssTable.close()
    }

    // Add next level entries (only if key not already present from current level)
    for (ssTable <- nextLevelTables) {
      for ((key, entry) <- ssTable.iterator) {
        val wrapped = new ByteArrayWrapper(key)
        if (!mergedEntries.containsKey(wrapped)) {
          mergedEntries.put(wrapped, entry)  // Keep older value only if no newer one
        }
      }
      ssTable.close()
    }

    // Create new SSTable at next level
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

  // extractVersionFromFileName: Parse version number from snapshot filename
  // Example: "snapshot_42" -> 42, "garbage" -> -1
  private def extractVersionFromFileName(name: String): Long = {
    try {
      if (name.startsWith("snapshot_")) name.stripPrefix("snapshot_").toLong
      else -1  // Not a snapshot file
    } catch {
      case _: NumberFormatException => -1
    }
  }

  // findClosestSnapshot: Find the newest snapshot at or before target version

  // Used during load() to find the best starting point for recovery.
  // Example: target=100, snapshots=[10, 50, 80] -> returns 80
  private def findClosestSnapshot(version: Long): Long = {
    val snapshotFiles = snapshotDir.listFiles()

    if (snapshotFiles == null || snapshotFiles.isEmpty) return 0  // No snapshots exist

    val versions = snapshotFiles
      .map(f => extractVersionFromFileName(f.getName))
      .filter(v => v >= 0 && v <= version)
      .sorted

    if (versions.isEmpty) 0 else versions.last
  }

  // loadSnapshot: Loads state from a snapshot file (streaming, memory-efficient)

  // Reads entries one by one from snapshot file without buffering all in memory.
  // Handles both new format (Long count) and legacy format (Int count) for
  // backwards compatibility during migration
  private def loadSnapshot(version: Long): Unit = {
    val snapshotFile = new File(snapshotDir, s"snapshot_$version")

    if (!snapshotFile.exists()) downloadSnapshot(version) // Try to download from DFS

    if (!snapshotFile.exists()) {
      throw new IOException(s"Snapshot file not found for version $version: $snapshotFile")
    }

    val input = new DataInputStream(
      new BufferedInputStream(new FileInputStream(snapshotFile), 256 * 1024)
    )
    try {
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

  // createSnapshot: Creates a local snapshot file by streaming data

  // This method streams data directly to disk instead of buffering all entries in memory.
  // This prevents OOM with large state.
  // I am writing entry count at the end as follows:
  // 1. Write all entries to temp file (streaming)
  // 2. Write final file with count header + data
  private def createSnapshot(version: Long): Unit = {
    val snapshotFile = new File(snapshotDir, s"snapshot_$version")
    val tempFile = new File(snapshotDir, s"snapshot_$version.tmp")

    // First, stream entries to temp file and count them
    var entryCount = 0L
    val tempOutput = new DataOutputStream(
      new BufferedOutputStream(new FileOutputStream(tempFile), 256 * 1024)
    )

    try {
      // Stream entries directly, no buffering in memory
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

    // Next is to write final file with count header
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
      tempFile.delete()
    }

    logDebug(s"[$loggingId] Created local snapshot $version with $entryCount entries")
  }

  // uploadSnapshot: Uploads snapshot to DFS with atomic write

  // This method fails the commit if upload fails.
  // I used Streaming's CheckpointFileManager.createAtomic for atomic write (temp + rename)
  private def uploadSnapshot(version: Long): Unit = {
    val localSnapshotFile = new File(snapshotDir, s"snapshot_$version")
    val dfsPath = new HadoopPath(dfsRootDir, s"snapshot_$version")

    // Verify local snapshot exists
    if (!localSnapshotFile.exists()) {
      throw new IOException(s"Local snapshot file not found: $localSnapshotFile")
    }

    // createAtomic writes to temp and then renames on close
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
      outputStream.flush()
    } catch {
      case e: Throwable =>
        // Cancel the atomic write and this deletes temp file
        try {
          outputStream.cancel()
        } catch {
          case NonFatal(_) => // Ignore cancel errors
        }
        // Throwing this to fail the commit
        throw new IOException(s"Failed to upload snapshot $version to DFS: ${e.getMessage}", e)
    } finally {
      outputStream.close()
    }

    logInfo(s"[$loggingId] Uploaded snapshot $version to DFS: $dfsPath")
  }

  // downloadSnapshot: Download a snapshot from DFS to local disk

  // Called when we need a snapshot that isn't in local cache.
  // Streams from DFS (HDFS/S3) to local file system.
  // Failure is logged but I am not failing the run. I am trying WAL replay.
  private def downloadSnapshot(version: Long): Unit = {
    val dfsPath = new HadoopPath(dfsRootDir, s"snapshot_$version")
    val localSnapshotFile = new File(snapshotDir, s"snapshot_$version")

    try {
      val inputStream = fm.open(dfsPath) // Stream from DFS to local file
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
        // Download failure is not fatal - we can try WAL replay
        logWarning(s"[$loggingId] Failed to download snapshot $version from DFS", e)
    }
  }

  // Override log name to include partition identifier for easier debugging
  override protected def logName: String = s"${super.logName} $loggingId"
}

object LSMTree {

  // ByteArrayWrapper: Makes byte arrays usable as map keys
  // I had to implement it because java collections can't use byte[] directly as keys.
  // This is because two byte arrays with same content are considered different.
  // So I wrap them so that equals() compares content and not the object identity.
  class ByteArrayWrapper(val data: Array[Byte]) {
    // Hash the byte array contents, not the array reference
    override def hashCode(): Int = JArrays.hashCode(data)

    // Compare byte array contents, not references
    override def equals(obj: Any): Boolean = obj match {
      case other: ByteArrayWrapper => JArrays.equals(data, other.data)
      case _ => false
    }
  }

  private object ByteArrayWrapper {
    // Comparator for TreeMap ordering (lexicographic byte comparison)
    val COMPARATOR: Comparator[ByteArrayWrapper] = (o1: ByteArrayWrapper, o2: ByteArrayWrapper) => {
      compareByteArrays(o1.data, o2.data)
    }
  }

  // compareByteArrays: lexicographic comparison of byte arrays

  // This is called many times during reads/compaction, so it's optimized:
  // - For small arrays (<8 bytes) through simple byte by byte comparison
  // - For large arrays by comparing 8 bytes at a time as Long
  // Returns: negative if a < b, 0 if a == b, positive if a > b
  def compareByteArrays(a: Array[Byte], b: Array[Byte]): Int = {
    val minLen = math.min(a.length, b.length)

    // For very small arrays, simple comparison is faster (no Long conversion overhead)
    if (minLen < 8) return compareByteArraysSimple(a, b, minLen)

    // Compare 8 bytes at a time using Long comparison
    // This reduces the number of comparisons for large keys.
    var offset = 0
    while (offset + 8 <= minLen) {
      val aLong = getLongBigEndian(a, offset)
      val bLong = getLongBigEndian(b, offset)
      // bytes are unsigned (0-255) so comparing with unsigned method
      if (aLong != bLong) return java.lang.Long.compareUnsigned(aLong, bLong)
      offset += 8
    }

    // Remaining bytes (less than 8)
    while (offset < minLen) {
      // Mask with 0xFF to treat bytes as unsigned (0-255)
      val diff = (a(offset) & 0xFF) - (b(offset) & 0xFF)
      if (diff != 0) return diff
      offset += 1
    }

    // Shorter array comes first when all bytes match
    // Example: "cat" < "cats" (like dictionary order)
    a.length - b.length
  }

  // compareByteArraysSimple: Simple byte-by-byte comparison for small arrays
  private def compareByteArraysSimple(a: Array[Byte], b: Array[Byte], minLen: Int): Int = {
    var i = 0
    while (i < minLen) {
      // 0xFF mask treats bytes as unsigned
      val diff = (a(i) & 0xFF) - (b(i) & 0xFF)
      if (diff != 0) return diff
      i += 1
    }
    a.length - b.length
  }

  // getLongBigEndian: Read 8 bytes as a big-endian Long for fast comparison
  // Big-endian means the most significant byte comes first.
  // This matches lexicographic ordering, so we can compare as Long directly
  private def getLongBigEndian(bytes: Array[Byte], offset: Int): Long = {
    // Shift each byte into its position in the Long (8 bytes = 64 bits)
    ((bytes(offset).toLong & 0xFF) << 56) |      // Most significant byte
      ((bytes(offset + 1).toLong & 0xFF) << 48) |
      ((bytes(offset + 2).toLong & 0xFF) << 40) |
      ((bytes(offset + 3).toLong & 0xFF) << 32) |
      ((bytes(offset + 4).toLong & 0xFF) << 24) |
      ((bytes(offset + 5).toLong & 0xFF) << 16) |
      ((bytes(offset + 6).toLong & 0xFF) << 8) |
      (bytes(offset + 7).toLong & 0xFF)          // Least significant byte
  }

  // ColumnFamilyState: Placeholder for column family metadata

  // Currently just a marker class. Could be extended to hold:
  // - Per-family statistics
  // - Schema information
  // - Compression settings
  private class ColumnFamilyState {
    // TODO: Add column family specific state if needed
  }
}

// WALOperation: Types of operations recorded in the Write-Ahead Log

// PUT = store a key-value pair
// DELETE = remove a key (stored as tombstone)
object WALOperation extends Enumeration {
  type WALOperation = Value
  val PUT, DELETE = Value  // Only two operations needed for a KV store
}

