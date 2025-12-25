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
    memTableSizeBytes: Long = 64 * 1024 * 1024,  // 64 MB default
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

  // ============================================================================
  // State Store Instance
  // ============================================================================

  private class LSMTreeStateStore(
                                   val version: Long,
                                   readOnly: Boolean
                                 ) extends StateStore {

    /** Trait and classes representing the internal state of the store */
    trait STATE
    case object UPDATING extends STATE
    case object COMMITTED extends STATE
    case object ABORTED extends STATE

    @volatile private var state: STATE = UPDATING
    @volatile private var hasCommittedOnce = false

    // Track modifications for this version
    private val pendingPuts = new ArrayBuffer[(Array[Byte], Array[Byte])]()
    private val pendingDeletes = new ArrayBuffer[Array[Byte]]()

    override def id: StateStoreId = LSMTreeStateStoreProvider.this.stateStoreId

    override def removeColFamilyIfExists(colFamilyName: String): Boolean = {
      if (colFamilyName != StateStore.DEFAULT_COL_FAMILY_NAME) {
        lsmTree.removeColumnFamily(colFamilyName)
      } else {
        false
      }
    }

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

    override def allColumnFamilyNames: Set[String] = lsmTree.getColumnFamilies

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

    override def put(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
      require(state == UPDATING, s"Cannot put in state $state")
      val keyBytes = key.getBytes.clone()
      val valueBytes = value.getBytes.clone()
      lsmTree.put(keyBytes, valueBytes, colFamilyName)
      pendingPuts += ((keyBytes, valueBytes))
    }

    override def putList(
        key: UnsafeRow,
        values: Array[UnsafeRow],
        colFamilyName: String): Unit = {
      throw StateStoreErrors.unsupportedOperationException("putList", "LSMTreeStateStore")
    }

    override def remove(key: UnsafeRow, colFamilyName: String): Unit = {
      require(state == UPDATING, s"Cannot remove in state $state")
      val keyBytes = key.getBytes.clone()
      lsmTree.delete(keyBytes, colFamilyName)
      pendingDeletes += keyBytes
    }

    override def merge(
        key: UnsafeRow,
        value: UnsafeRow,
        colFamilyName: String): Unit = {
      // For LSM-Tree, merge is implemented as put (last-write-wins)
      put(key, value, colFamilyName)
    }

    override def mergeList(
        key: UnsafeRow,
        values: Array[UnsafeRow],
        colFamilyName: String): Unit = {
      throw StateStoreErrors.unsupportedOperationException("mergeList", "LSMTreeStateStore")
    }

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

    override def valuesIterator(
        key: UnsafeRow,
        colFamilyName: String): Iterator[UnsafeRow] = {
      throw StateStoreErrors.unsupportedOperationException("valuesIterator", "LSMTreeStateStore")
    }

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

        newVersion
      } catch {
        case e: Throwable =>
          logError(s"[$stateStoreId] STORE COMMIT FAILED: version=${version + 1}", e)
          abort()
          throw e
      }
    }

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

    override def getStateStoreCheckpointInfo(): StateStoreCheckpointInfo = {
      StateStoreCheckpointInfo(
        partitionId = id.partitionId,
        batchVersion = version,
        stateStoreCkptId = None,
        baseStateStoreCkptId = None
      )
    }

    override def hasCommitted: Boolean = hasCommittedOnce
  }

  // ============================================================================
  // Provider State
  // ============================================================================

  @volatile private var stateStoreId_ : StateStoreId = _
  @volatile private var keySchema: StructType = _
  @volatile private var valueSchema: StructType = _
  @volatile private var storeConf: StateStoreConf = _
  @volatile private var hadoopConf: Configuration = _
  @volatile private var lsmConf: LSMTreeConf = _

  private var fm: CheckpointFileManager = _
  private var lsmTree: LSMTree = _
  private var stateStoreProviderId: StateStoreProviderId = _
  private var useColumnFamilies: Boolean = false

  // ============================================================================
  // StateStoreProvider Implementation
  // ============================================================================

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

    // Initialize checkpoint file manager
    this.fm = CheckpointFileManager.create(
      new Path(stateStoreId.storeCheckpointLocation().toString),
      hadoopConf)

    // Create local working directory for LSM-Tree
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)
    val storeIdStr = s"LSMTree-${stateStoreId.operatorId}-${stateStoreId.partitionId}"
    val localRootDir = Utils.createExecutorLocalTempDir(sparkConf, storeIdStr)

    // Initialize the LSM-Tree
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

  override def stateStoreId: StateStoreId = stateStoreId_

  override def getStore(
      version: Long,
      stateStoreCkptId: Option[String] = None,
      forceSnapshotOnCommit: Boolean = false,
      loadEmpty: Boolean = false): StateStore = {
    loadStoreInternal(version, readOnly = false, loadEmpty = loadEmpty)
  }

  override def getReadStore(version: Long, stateStoreCkptId: Option[String]): ReadStateStore = {
    loadStoreInternal(version, readOnly = true)
  }

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

  override def doMaintenance(): Unit = {
    val startTime = System.currentTimeMillis()
    logDebug(s"[$stateStoreProviderId] MAINTENANCE START")

    try {
      val compactionStart = System.currentTimeMillis()
      lsmTree.runCompaction()
      val compactionTime = System.currentTimeMillis() - compactionStart

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

  override def close(): Unit = {
    if (lsmTree != null) {
      lsmTree.close()
    }
    if (fm != null) {
      fm.close()
    }
    logInfo(s"Closed LSMTreeStateStoreProvider for $stateStoreProviderId")
  }

  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = {
    CUSTOM_METRICS
  }

  override protected def logName: String = s"${super.logName} $stateStoreProviderId"

  // Fine-grained replay support (SupportsFineGrainedReplay trait)
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

/**
 * Iterator for reading changelog data from LSM-Tree.
 */
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
    // No resources to close
  }
}

object LSMTreeStateStoreProvider {
  // Custom metrics
  val METRIC_MEMTABLE_SIZE = StateStoreCustomSumMetric(
    "lsmtreeMemTableSizeBytes",
    "Size of the active MemTable in bytes")

  val METRIC_SSTABLE_COUNT = StateStoreCustomSumMetric(
    "lsmtreeSsTableCount",
    "Number of SSTables across all levels")

  val METRIC_BLOOM_FILTER_HIT_RATE = StateStoreCustomSumMetric(
    "lsmtreeBloomFilterHitRate",
    "Bloom filter hit rate (avoided disk reads)")

  val METRIC_COMPACTION_TIME_MS = StateStoreCustomTimingMetric(
    "lsmtreeCompactionTimeMs",
    "Time spent in compaction (ms)")

  val METRIC_WAL_SIZE_BYTES = StateStoreCustomSumMetric(
    "lsmtreeWalSizeBytes",
    "Size of Write-Ahead Log in bytes")

  private val CUSTOM_METRICS: Seq[StateStoreCustomMetric] = Seq(
    METRIC_MEMTABLE_SIZE,
    METRIC_SSTABLE_COUNT,
    METRIC_BLOOM_FILTER_HIT_RATE,
    METRIC_COMPACTION_TIME_MS,
    METRIC_WAL_SIZE_BYTES
  )
}

/**
 * Statistics for the LSM-Tree.
 */
case class LSMTreeStats(
    numKeys: Long,
    memoryUsedBytes: Long,
    memTableSizeBytes: Long,
    ssTableCount: Long,
    bloomFilterHitRate: Long,
    lastCompactionTimeMs: Long,
    walSizeBytes: Long
)
