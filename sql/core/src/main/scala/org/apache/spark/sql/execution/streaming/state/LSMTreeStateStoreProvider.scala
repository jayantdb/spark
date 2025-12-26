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
import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.streaming.checkpointing.CheckpointFileManager
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{NextIterator, Utils}

/**
 * Configuration for LSMTree StateStore.
 *
 * @param memTableSizeBytes Maximum size of the in-memory MemTable before flushing to disk
 * @param blockSizeBytes Size of each data block in SSTables
 * @param bloomFilterFpp False positive probability for Bloom filters
 * @param maxLevels Maximum number of levels in the LSM-Tree
 * @param levelSizeMultiplier Size multiplier between levels (typically 10)
 * @param maxMemTableCount Maximum number of immutable MemTables before blocking writes
 * @param compactionThreads Number of threads for background compaction
 * @param walEnabled Whether Write-Ahead Log is enabled
 * @param sparseIndexInterval Interval for sparse index entries
 */
case class LSMTreeConf(
    memTableSizeBytes: Long = 64 * 1024 * 1024,   // 64 MB default
    blockSizeBytes: Int = 4 * 1024,               // 4 KB blocks
    bloomFilterFpp: Double = 0.01,                // 1% false positive rate
    maxLevels: Int = 7,                           // Support up to 10^7 * 64MB = 640TB
    levelSizeMultiplier: Int = 10,
    maxMemTableCount: Int = 4,
    compactionThreads: Int = 2,
    walEnabled: Boolean = true,
    sparseIndexInterval: Int = 16                 // One index entry per 16 keys
)

object LSMTreeConf {
  def apply(storeConf: StateStoreConf): LSMTreeConf = {
    val sqlConfs = storeConf.sqlConfs
    LSMTreeConf(
      memTableSizeBytes = sqlConfs.getOrElse(
        "spark.sql.streaming.stateStore.lsmtree.memTableSizeBytes", "67108864").toLong,
      blockSizeBytes = sqlConfs.getOrElse(
        "spark.sql.streaming.stateStore.lsmtree.blockSizeBytes", "4096").toInt,
      bloomFilterFpp = sqlConfs.getOrElse(
        "spark.sql.streaming.stateStore.lsmtree.bloomFilterFpp", "0.01").toDouble,
      maxLevels = sqlConfs.getOrElse(
        "spark.sql.streaming.stateStore.lsmtree.maxLevels", "7").toInt,
      levelSizeMultiplier = sqlConfs.getOrElse(
        "spark.sql.streaming.stateStore.lsmtree.levelSizeMultiplier", "10").toInt,
      maxMemTableCount = sqlConfs.getOrElse(
        "spark.sql.streaming.stateStore.lsmtree.maxMemTableCount", "4").toInt,
      compactionThreads = sqlConfs.getOrElse(
        "spark.sql.streaming.stateStore.lsmtree.compactionThreads", "2").toInt,
      walEnabled = sqlConfs.getOrElse(
        "spark.sql.streaming.stateStore.lsmtree.walEnabled", "true").toBoolean,
      sparseIndexInterval = sqlConfs.getOrElse(
        "spark.sql.streaming.stateStore.lsmtree.sparseIndexInterval", "16").toInt
    )
  }
}

/**
 * High-performance LSM-Tree based StateStore implementation.
 *
 * This implementation eliminates JNI overhead by using pure Scala/JVM constructs:
 * - Off-heap memory via DirectByteBuffers for zero-copy operations
 * - Memory-mapped files (mmap) for fast disk I/O
 * - ConcurrentSkipListMap for lock-free MemTable operations
 * - Bloom filters to skip unnecessary disk reads
 * - Write-Ahead Log (WAL) for fault tolerance
 * - Tiered compaction for handling TBs of state
 *
 * Architecture:
 * {{{
 *   Write Path:  put() -> MemTable (off-heap SkipList) -> WAL
 *                            |
 *                      [Flush when full]
 *                            |
 *                            v
 *                     SSTable (Level 0) -> Compaction -> Higher Levels
 *
 *   Read Path:   get() -> MemTable -> Immutable MemTables -> L0 -> L1 -> ... -> Ln
 *                            ^              ^                 ^
 *                       [In-Memory]    [Bloom Filter]    [Sparse Index]
 * }}}
 */
private[sql] class LSMTreeStateStoreProvider
  extends StateStoreProvider with Logging with Closeable
  with SupportsFineGrainedReplay {

  import LSMTreeStateStoreProvider._

  // LSMTreeStateStore - Inner Class
  // This is the actual StateStore instance returned to Spark for each partition.
  // It wraps the LSMTree and provides the StateStore interface (get/put/remove).
  // Each instance operates on a specific version of the state.
  private class LSMTreeStateStore(
                                   val version: Long,
                                   readOnly: Boolean
                                 ) extends StateStore {
    trait STATE
    case object UPDATING extends STATE // Store is open for modifications (put/remove)
    case object COMMITTED extends STATE // Store has been successfully committed (read-only now)
    case object ABORTED extends STATE // Store was aborted, all pending changes rolled back

    @volatile private var state: STATE = UPDATING
    @volatile private var hasCommittedOnce = false

    // Track modifications for this version (used for rollback on abort)
    private val pendingPuts = new ArrayBuffer[(Array[Byte], Array[Byte])]()
    private val pendingDeletes = new ArrayBuffer[Array[Byte]]()

    // id: Returns the unique id for this state store partition
    // Used by Spark to track which partition this store belongs to
    override def id: StateStoreId = LSMTreeStateStoreProvider.this.stateStoreId

    // allColumnFamilyNames: Returns all column family names in this store
    override def allColumnFamilyNames: Set[String] = lsmTree.getColumnFamilies

    // createColFamilyIfAbsent: Creates a new column family if it doesn't exist
    // Column families are like separate "tables" within the same partition
    // Used for multi-state operations (e.g., session windows need multiple states)
    override def createColFamilyIfAbsent(
                                          colFamilyName: String,
                                          keySchema: StructType,
                                          valueSchema: StructType,
                                          keyStateEncoderSpec: KeyStateEncoderSpec,
                                          useMultipleValuesPerKey: Boolean = false,
                                          isInternal: Boolean = false): Unit = {
      if (colFamilyName != StateStore.DEFAULT_COL_FAMILY_NAME) {
        lsmTree.createColumnFamily(colFamilyName)
      }
    }

    // removeColFamilyIfExists: Removes a column family (logical namespace)
    // Column families allow storing different types of state separately
    override def removeColFamilyIfExists(colFamilyName: String): Boolean = {
      if (colFamilyName != StateStore.DEFAULT_COL_FAMILY_NAME) {
        lsmTree.removeColumnFamily(colFamilyName)
      } else {
        false
      }
    }

    // get: Retrieves value for a key from state store
    // Read path: MemTable -> Immutable MemTables -> SSTables (with Bloom filter)
    // Returns null if key doesn't exist
    override def get(key: UnsafeRow, colFamilyName: String): UnsafeRow = {
      val keyBytes = key.getBytes
      val valueBytes = lsmTree.get(keyBytes, colFamilyName)
      if (valueBytes == null) {
        null
      } else {
        val row = new UnsafeRow(valueSchema.length)
        row.pointTo(valueBytes, valueBytes.length)
        row
      }
    }

    // put: Stores a key-value pair in state store
    // Write path: MemTable -> WAL (for durability) -> eventually SSTable
    // Only allowed when store is in UPDATING state
    override def put(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
      require(state == UPDATING, s"Cannot put in state $state")
      val keyBytes = key.getBytes.clone()
      val valueBytes = value.getBytes.clone()
      lsmTree.put(keyBytes, valueBytes, colFamilyName)
      pendingPuts += ((keyBytes, valueBytes))
    }

    // putList: Stores multiple values for a single key.
    // Currently, the LSM-Tree implementation uses a single-value-per-key model.
    // Jayant - Todo: I will implement it later. I need to understand how rocksDB has done it.
    override def putList(
        key: UnsafeRow,
        values: Array[UnsafeRow],
        colFamilyName: String): Unit = {
      throw StateStoreErrors.unsupportedOperationException("putList", "LSMTreeStateStore")
    }

    // remove: Deletes a key from state store. Only allowed when store is in UPDATING state
    // It only writes a "tombstone" marker; actual deletion happens during compaction.
    // I am calling soft deletes as tombstone.
    // Its more like a common compute term to represent died object
    override def remove(key: UnsafeRow, colFamilyName: String): Unit = {
      require(state == UPDATING, s"Cannot remove in state $state")
      val keyBytes = key.getBytes.clone()
      lsmTree.delete(keyBytes, colFamilyName)
      pendingDeletes += keyBytes
    }

    // merge: Merges value with existing value for a key
    // In LSM-Tree, implemented as simple put (last-write-wins semantics)
    override def merge(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit =
      put(key, value, colFamilyName)

    // mergeList: Merges list values with existing values (not supported)
    // Todo: Will think about it in future.
    override def mergeList(
        key: UnsafeRow,
        values: Array[UnsafeRow],
        colFamilyName: String): Unit = {
      throw StateStoreErrors.unsupportedOperationException("mergeList", "LSMTreeStateStore")
    }

    // iterator: Returns iterator over all key-value pairs in the store
    // Merges data from MemTable and all SSTable levels in sorted order
    // Used by Spark for aggregations and state expiration
    override def iterator(colFamilyName: String): StateStoreIterator[UnsafeRowPair] = {
      val rawIterator = lsmTree.iterator(colFamilyName)
      val transformedIterator = rawIterator.map { case (keyBytes, valueBytes) =>
        val keyRow = new UnsafeRow(keySchema.length)
        keyRow.pointTo(keyBytes, keyBytes.length)
        val valueRow = new UnsafeRow(valueSchema.length)
        valueRow.pointTo(valueBytes, valueBytes.length)
        new UnsafeRowPair(keyRow, valueRow)
      }
      new StateStoreIterator(transformedIterator)
    }

    // prefixScan: Returns iterator for keys matching a prefix
    // Efficient for session windows and grouping operations
    // Uses sorted order of LSM-Tree for prefix matching
    override def prefixScan(
        prefixKey: UnsafeRow,
        colFamilyName: String): StateStoreIterator[UnsafeRowPair] = {
      val prefixBytes = prefixKey.getBytes
      val rawIterator = lsmTree.prefixScan(prefixBytes, colFamilyName)
      val transformedIterator = rawIterator.map { case (keyBytes, valueBytes) =>
        val keyRow = new UnsafeRow(keySchema.length)
        keyRow.pointTo(keyBytes, keyBytes.length)
        val valueRow = new UnsafeRow(valueSchema.length)
        valueRow.pointTo(valueBytes, valueBytes.length)
        new UnsafeRowPair(keyRow, valueRow)
      }
      new StateStoreIterator(transformedIterator)
    }

    // valuesIterator: Returns iterator for multiple values per key (not supported)
    // Todo
    override def valuesIterator(
        key: UnsafeRow,
        colFamilyName: String): Iterator[UnsafeRow] = {
      throw StateStoreErrors.unsupportedOperationException("valuesIterator", "LSMTreeStateStore")
    }

    // commit: Persists all changes and creates a new version
    // Flushes MemTable if needed, syncs WAL, reports to coordinator
    // Transitions state from UPDATING -> COMMITTED
    // Returns the new version number
    override def commit(): Long = {
      require(state == UPDATING, s"Cannot commit in state $state")
      val startTime = System.currentTimeMillis()
      val putsCount = pendingPuts.size
      val deletesCount = pendingDeletes.size

      logInfo(s"[$stateStoreId] STORE COMMIT START: version=${version + 1}, " +
        s"puts=$putsCount, deletes=$deletesCount")

      try {
        val newVersion = version + 1
        lsmTree.commit(newVersion)
        state = COMMITTED
        hasCommittedOnce = true
        pendingPuts.clear()
        pendingDeletes.clear()

        val duration = System.currentTimeMillis() - startTime
        logInfo(s"[$stateStoreId] STORE COMMIT COMPLETE: version=$newVersion, " +
          s"puts=$putsCount, deletes=$deletesCount, duration=${duration}ms")

        // Report the commit to StateStoreCoordinator for tracking
        // This is required for batch commit validation in Spark 4.0+
        if (storeConf.commitValidationEnabled) {
          StateStore.reportCommitToCoordinator(newVersion, stateStoreId, hadoopConf)
        }

        newVersion
      } catch {
        case e: Throwable =>
          logError(s"[$stateStoreId] STORE COMMIT FAILED: version=${version + 1}", e)
          abort()
          throw e
      }
    }

    // abort: Rolls back all uncommitted changes
    // Discards pending puts/deletes, resets MemTable state
    // Transitions state to ABORTED
    override def abort(): Unit = {
      if (state != ABORTED) {
        val putsCount = pendingPuts.size
        val deletesCount = pendingDeletes.size

        state = ABORTED

        // Rollback pending changes
        lsmTree.rollback()
        pendingPuts.clear()
        pendingDeletes.clear()

        logWarning(s"[$stateStoreId] STORE ABORTED: version=$version, " +
          s"rolledBackPuts=$putsCount, rolledBackDeletes=$deletesCount")
      }
    }

    // metrics: Returns performance metrics for monitoring
    // Exposed in Spark UI and available via StateStoreMetrics API
    // Includes: numKeys, memoryUsed, memTableSize, ssTableCount, etc.
    override def metrics: StateStoreMetrics = {
      val stats = lsmTree.getStats
      StateStoreMetrics(
        numKeys = stats.numKeys,
        memoryUsedBytes = stats.memoryUsedBytes,
        customMetrics = Map(
          METRIC_MEMTABLE_SIZE -> stats.memTableSizeBytes,
          METRIC_SSTABLE_COUNT -> stats.ssTableCount,
          METRIC_BLOOM_FILTER_HIT_RATE -> stats.bloomFilterHitRate,
          METRIC_COMPACTION_TIME_MS -> stats.lastCompactionTimeMs,
          METRIC_WAL_SIZE_BYTES -> stats.walSizeBytes
        ),
        instanceMetrics = Map.empty
      )
    }

    // getStateStoreCheckpointInfo: Returns checkpoint metadata
    // Used by Spark for checkpoint coordination and recovery
    override def getStateStoreCheckpointInfo(): StateStoreCheckpointInfo = {
      StateStoreCheckpointInfo(
        partitionId = id.partitionId,
        batchVersion = version,
        stateStoreCkptId = None,
        baseStateStoreCkptId = None
      )
    }

    // hasCommitted: Returns true if commit() was ever called successfully
    // Used by Spark to verify state store lifecycle
    override def hasCommitted: Boolean = hasCommittedOnce
  }

  // Provider State - Instance Variables
  // These are initialized in init() and used throughout the provider lifecycle.
  // One provider instance is created per partition per query.
  @volatile private var stateStoreId_ : StateStoreId = _     // Unique ID for this partition's store
  @volatile private var keySchema: StructType = _            // Schema for state keys
  @volatile private var valueSchema: StructType = _          // Schema for state values
  @volatile private var storeConf: StateStoreConf = _        // General state store configuration
  @volatile private var hadoopConf: Configuration = _        // Hadoop/HDFS configuration
  @volatile private var lsmConf: LSMTreeConf = _             // LSM-Tree specific configuration

  private var fm: CheckpointFileManager = _                  // Manages checkpoint files on DFS
  private var lsmTree: LSMTree = _                           // The actual LSM-Tree data structure
  private var stateStoreProviderId: StateStoreProviderId = _ // Provider ID including query run ID
  private var useColumnFamilies: Boolean = false             // Whether column families are enabled

  // StateStoreProvider Implementation
  // init: Called once when Spark creates the provider
  // Sets up LSM-Tree, local directories, checkpoint manager, and configuration
  // This is the entry point for the entire state store lifecycle
  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      storeConfs: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false,
      stateSchemaProvider: Option[StateSchemaProvider] = None): Unit = {

    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.storeConf = storeConfs
    this.hadoopConf = hadoopConf
    this.useColumnFamilies = useColumnFamilies
    this.lsmConf = LSMTreeConf(storeConfs)

    val queryRunId = UUID.fromString(StateStoreProvider.getRunId(hadoopConf))
    this.stateStoreProviderId = StateStoreProviderId(stateStoreId, queryRunId)

    // Initialize checkpoint file manager for DFS operations
    this.fm = CheckpointFileManager.create(
      new Path(stateStoreId.storeCheckpointLocation().toString),
      hadoopConf)

    // Create local working directory for LSM-Tree (SSTables, WAL, etc.)
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)
    val storeIdStr = s"LSMTree-${stateStoreId.operatorId}-${stateStoreId.partitionId}"
    val localRootDir = Utils.createTempDir(Utils.getLocalDir(sparkConf), storeIdStr)

    // Initialize the LSM-Tree with all configuration
    this.lsmTree = new LSMTree(
      localRootDir = localRootDir,
      dfsRootDir = stateStoreId.storeCheckpointLocation().toString,
      conf = lsmConf,
      fm = fm,
      hadoopConf = hadoopConf,
      loggingId = stateStoreProviderId.toString
    )

    fm.mkdirs(new Path(stateStoreId.storeCheckpointLocation().toString))

    logInfo(s"[$stateStoreProviderId] PROVIDER INITIALIZED: " +
      s"operator=${stateStoreId.operatorId}, partition=${stateStoreId.partitionId}, " +
      s"memTableSize=${lsmConf.memTableSizeBytes / 1024 / 1024}MB, " +
      s"blockSize=${lsmConf.blockSizeBytes / 1024}KB, " +
      s"bloomFilterFpp=${lsmConf.bloomFilterFpp}, maxLevels=${lsmConf.maxLevels}, " +
      s"walEnabled=${lsmConf.walEnabled}")
  }

  // stateStoreId: Returns the unique Id for this state store partition
  // Includes: operator ID, partition ID, store name, checkpoint location
  override def stateStoreId: StateStoreId = stateStoreId_

  // getStore: Returns a writable StateStore for the given version
  // Called by Spark at the start of each micro-batch to get state for updates
  // Loads state from checkpoint and prepares for put/remove operations
  override def getStore(
      version: Long,
      stateStoreCkptId: Option[String] = None,
      forceSnapshotOnCommit: Boolean = false,
      loadEmpty: Boolean = false): StateStore = {
    loadStoreInternal(version, readOnly = false, loadEmpty = loadEmpty)
  }

  // getReadStore: Returns a read-only StateStore for the given version
  // Used for state inspection without modification (e.g., UI, debugging)
  override def getReadStore(version: Long, stateStoreCkptId: Option[String]): ReadStateStore = {
    loadStoreInternal(version, readOnly = true)
  }

  // loadStoreInternal: Common logic for loading a store (read or write mode)
  // Loads LSM-Tree state from checkpoint/snapshot and creates StateStore instance
  private def loadStoreInternal(
      version: Long,
      readOnly: Boolean,
      loadEmpty: Boolean = false): LSMTreeStateStore = {
    val startTime = System.currentTimeMillis()
    val mode = if (readOnly) "READ_ONLY" else "READ_WRITE"

    logInfo(s"[$stateStoreProviderId] LOAD_STORE START: version=$version, mode=$mode, " +
      s"loadEmpty=$loadEmpty")

    try {
      if (version < 0) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(version)
      }

      if (loadEmpty) {
        // Start with empty state (for schema evolution or recovery scenarios)
        logDebug(s"[$stateStoreProviderId] Loading empty state for schema evolution")
        lsmTree.load(0)
      } else {
        lsmTree.load(version)
      }

      val store = new LSMTreeStateStore(version, readOnly)
      val duration = System.currentTimeMillis() - startTime

      logInfo(s"[$stateStoreProviderId] LOAD_STORE COMPLETE: version=$version, " +
        s"mode=$mode, duration=${duration}ms")

      store
    } catch {
      case e: OutOfMemoryError =>
        logError(s"[$stateStoreProviderId] LOAD_STORE FAILED: OOM loading version $version", e)
        throw QueryExecutionErrors.notEnoughMemoryToLoadStore(
          stateStoreId.toString,
          "LSMTREE_STORE_PROVIDER",
          e)
      case e: Throwable =>
        logError(s"[$stateStoreProviderId] LOAD_STORE FAILED: version=$version", e)
        throw StateStoreErrors.cannotLoadStore(e)
    }
  }

  // doMaintenance: Called periodically by Spark's maintenance thread
  // Performs background tasks: compaction, cleanup of old versions
  // Important for long-running queries to prevent unbounded disk usage
  override def doMaintenance(): Unit = {
    val startTime = System.currentTimeMillis()
    logDebug(s"[$stateStoreProviderId] MAINTENANCE START")

    try {
      // Compaction: merge SSTables to reduce read amplification
      val compactionStart = System.currentTimeMillis()
      lsmTree.runCompaction()
      val compactionTime = System.currentTimeMillis() - compactionStart

      // Cleanup: remove old versions beyond retention period
      val cleanupStart = System.currentTimeMillis()
      lsmTree.cleanupOldVersions(storeConf.minVersionsToRetain)
      val cleanupTime = System.currentTimeMillis() - cleanupStart

      val totalTime = System.currentTimeMillis() - startTime
      val stats = lsmTree.getStats

      logInfo(s"[$stateStoreProviderId] MAINTENANCE COMPLETE: " +
        s"compaction=${compactionTime}ms, cleanup=${cleanupTime}ms, total=${totalTime}ms, " +
        s"ssTableCount=${stats.ssTableCount}, memoryUsed=${stats.memoryUsedBytes}B")
    } catch {
      case NonFatal(e) =>
        logWarning(s"[$stateStoreProviderId] MAINTENANCE FAILED", e)
    }
  }

  // close: Called when query terminates or executor is being shut down
  // Releases all resources: closes LSM-Tree, file handles, memory mappings
  override def close(): Unit = {
    if (lsmTree != null) {
      lsmTree.close()
    }
    if (fm != null) {
      fm.close()
    }
    logInfo(s"Closed LSMTreeStateStoreProvider for $stateStoreProviderId")
  }

  // supportedCustomMetrics: Returns list of custom metrics this provider exposes
  // Shown in Spark UI and available via metrics API
  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = {
    CUSTOM_METRICS
  }

  // logName: Custom log name for easier debugging
  // Includes provider ID for correlation with partition-specific logs
  override protected def logName: String = s"${super.logName} $stateStoreProviderId"

  // SupportsFineGrainedReplay Implementation
  // These methods enable efficient state recovery by replaying only the changes (deltas)
  // since the last snapshot, instead of loading full state.

  // replayStateFromSnapshot: Loads snapshot and replays changes up to endVersion
  // More efficient than loading full state for each version
  // Used during recovery after failures or restarts
  override def replayStateFromSnapshot(
      snapshotVersion: Long,
      endVersion: Long,
      readOnly: Boolean = false,
      snapshotVersionStateStoreCkptId: Option[String] = None,
      endVersionStateStoreCkptId: Option[String] = None): StateStore = {
    lsmTree.load(snapshotVersion)
    // Replay from changelog
    lsmTree.replayFromVersion(snapshotVersion, endVersion)
    new LSMTreeStateStore(endVersion, readOnly)
  }

  // getStateStoreChangeDataReader: Returns iterator over state changes
  // Used for state change logging and incremental processing
  // Returns (RecordType, key, value, version) tuples
  override def getStateStoreChangeDataReader(
      startVersion: Long,
      endVersion: Long,
      colFamilyNameOpt: Option[String] = None,
      endVersionStateStoreCkptId: Option[String] = None):
      NextIterator[(RecordType.Value, UnsafeRow, UnsafeRow, Long)] = {
    new LSMTreeChangeDataReader(
      lsmTree,
      startVersion,
      endVersion,
      colFamilyNameOpt,
      keySchema,
      valueSchema
    )
  }

}

// LSMTreeChangeDataReader - Helper Class for State Change Iteration
// This iterator reads state changes (puts and deletes) between versions.
// Used by Spark for state change logging and incremental state inspection.
// Returns tuples of (RecordType, keyRow, valueRow, version).
private class LSMTreeChangeDataReader(
    lsmTree: LSMTree,
    startVersion: Long,
    endVersion: Long,
    colFamilyNameOpt: Option[String],
    keySchema: StructType,
    valueSchema: StructType)
    extends NextIterator[(RecordType.Value, UnsafeRow, UnsafeRow, Long)] {

  private var currentVersion = startVersion
  private var currentIterator: Iterator[(Array[Byte], Array[Byte])] = Iterator.empty

  // getNext: Returns the next state change record
  // Iterates through all versions, returning PUT or DELETE records
  override protected def getNext(): (RecordType.Value, UnsafeRow, UnsafeRow, Long) = {
    while (currentVersion <= endVersion) {
      if (currentIterator.hasNext) {
        val (keyBytes, valueBytes) = currentIterator.next()
        val keyRow = new UnsafeRow(keySchema.length)
        keyRow.pointTo(keyBytes, keyBytes.length)

        if (valueBytes != null) {
          val valueRow = new UnsafeRow(valueSchema.length)
          valueRow.pointTo(valueBytes, valueBytes.length)
          return (RecordType.PUT_RECORD, keyRow, valueRow, currentVersion)
        } else {
          return (RecordType.DELETE_RECORD, keyRow, null, currentVersion)
        }
      } else {
        currentVersion += 1
        if (currentVersion <= endVersion) {
          lsmTree.load(currentVersion)
          currentIterator = lsmTree.iterator(
            colFamilyNameOpt.getOrElse(StateStore.DEFAULT_COL_FAMILY_NAME))
        }
      }
    }
    finished = true
    null
  }

  override protected def close(): Unit = {
    // No resources to close, leaving this empty
  }
}

// LSMTreeStateStoreProvider
// Contains constants and metric definitions shared across all provider instances.
// These metrics are exposed in Spark UI under the Streaming tab.
object LSMTreeStateStoreProvider {

  // Custom Metrics - Exposed in Spark UI for monitoring LSM-Tree performance

  // Tracks size of the in-memory buffer (MemTable)
  // High values indicate more data waiting to be flushed to disk
  val METRIC_MEMTABLE_SIZE = StateStoreCustomSumMetric(
    "lsmtreeMemTableSizeBytes",
    "Size of the active MemTable in bytes")

  // Counts total SSTables across all levels
  // High count may indicate need for compaction
  val METRIC_SSTABLE_COUNT = StateStoreCustomSumMetric(
    "lsmtreeSsTableCount",
    "Number of SSTables across all levels")

  // Percentage of reads that avoided disk I/O due to Bloom filter
  // Higher is better (100% = all negative lookups caught by Bloom filter)
  val METRIC_BLOOM_FILTER_HIT_RATE = StateStoreCustomSumMetric(
    "lsmtreeBloomFilterHitRate",
    "Bloom filter hit rate (avoided disk reads)")

  // Time spent in background compaction
  // High values may indicate I/O bottleneck or large state
  val METRIC_COMPACTION_TIME_MS = StateStoreCustomTimingMetric(
    "lsmtreeCompactionTimeMs",
    "Time spent in compaction (ms)")

  // Size of Write-Ahead Log files
  // Grows between MemTable flushes, cleaned up after flush
  val METRIC_WAL_SIZE_BYTES = StateStoreCustomSumMetric(
    "lsmtreeWalSizeBytes",
    "Size of Write-Ahead Log in bytes")

  // All metrics collected and exposed to Spark
  private val CUSTOM_METRICS: Seq[StateStoreCustomMetric] = Seq(
    METRIC_MEMTABLE_SIZE,
    METRIC_SSTABLE_COUNT,
    METRIC_BLOOM_FILTER_HIT_RATE,
    METRIC_COMPACTION_TIME_MS,
    METRIC_WAL_SIZE_BYTES
  )
}

// LSMTreeStats - Statistics Container
// Holds a snapshot of LSM-Tree statistics at a point in time.
// Used for metrics reporting and debugging.
case class LSMTreeStats(
    numKeys: Long,              // Approximate number of unique keys in store
    memoryUsedBytes: Long,      // Total memory used by MemTable and caches
    memTableSizeBytes: Long,    // Current size of active MemTable
    ssTableCount: Long,         // Number of SSTables on disk
    bloomFilterHitRate: Long,   // Bloom filter effectiveness (0-100%)
    lastCompactionTimeMs: Long, // Duration of last compaction run
    walSizeBytes: Long          // Current WAL size on disk
)
