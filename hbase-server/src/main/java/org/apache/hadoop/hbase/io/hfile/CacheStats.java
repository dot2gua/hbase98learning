/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io.hfile;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Class that implements cache metrics.
 */
@InterfaceAudience.Private
public class CacheStats {
  /** Sliding window statistics. The number of metric periods to include in
   * sliding window hit ratio calculations.
   */
  static final int DEFAULT_WINDOW_PERIODS = 5;

  /** The number of getBlock requests that were cache hits */
  private final AtomicLong hitCount = new AtomicLong(0);

  /**
   * The number of getBlock requests that were cache hits, but only from
   * requests that were set to use the block cache.  This is because all reads
   * attempt to read from the block cache even if they will not put new blocks
   * into the block cache.  See HBASE-2253 for more information.
   */
  private final AtomicLong hitCachingCount = new AtomicLong(0);

  /** The number of getBlock requests that were cache misses */
  private final AtomicLong missCount = new AtomicLong(0);

  /**
   * The number of getBlock requests that were cache misses, but only from
   * requests that were set to use the block cache.
   */
  private final AtomicLong missCachingCount = new AtomicLong(0);

  /** The number of times an eviction has occurred */
  private final AtomicLong evictionCount = new AtomicLong(0);

  /** The total number of blocks that have been evicted */
  private final AtomicLong evictedBlockCount = new AtomicLong(0);

  /** The total number of blocks that were not inserted. */
  private final AtomicLong failedInserts = new AtomicLong(0);

  /** Per Block Type Counts */
  private final AtomicLong dataMissCount = new AtomicLong(0);
  private final AtomicLong leafIndexMissCount = new AtomicLong(0);
  private final AtomicLong bloomChunkMissCount = new AtomicLong(0);
  private final AtomicLong metaMissCount = new AtomicLong(0);
  private final AtomicLong rootIndexMissCount = new AtomicLong(0);
  private final AtomicLong intermediateIndexMissCount = new AtomicLong(0);
  private final AtomicLong fileInfoMissCount = new AtomicLong(0);
  private final AtomicLong generalBloomMetaMissCount = new AtomicLong(0);
  private final AtomicLong deleteFamilyBloomMissCount = new AtomicLong(0);
  private final AtomicLong trailerMissCount = new AtomicLong(0);

  private final AtomicLong dataHitCount = new AtomicLong(0);
  private final AtomicLong leafIndexHitCount = new AtomicLong(0);
  private final AtomicLong bloomChunkHitCount = new AtomicLong(0);
  private final AtomicLong metaHitCount = new AtomicLong(0);
  private final AtomicLong rootIndexHitCount = new AtomicLong(0);
  private final AtomicLong intermediateIndexHitCount = new AtomicLong(0);
  private final AtomicLong fileInfoHitCount = new AtomicLong(0);
  private final AtomicLong generalBloomMetaHitCount = new AtomicLong(0);
  private final AtomicLong deleteFamilyBloomHitCount = new AtomicLong(0);
  private final AtomicLong trailerHitCount = new AtomicLong(0);

  /** The number of metrics periods to include in window */
  private final int numPeriodsInWindow;
  /** Hit counts for each period in window */
  private final long [] hitCounts;
  /** Caching hit counts for each period in window */
  private final long [] hitCachingCounts;
  /** Access counts for each period in window */
  private final long [] requestCounts;
  /** Caching access counts for each period in window */
  private final long [] requestCachingCounts;
  /** Last hit count read */
  private long lastHitCount = 0;
  /** Last hit caching count read */
  private long lastHitCachingCount = 0;
  /** Last request count read */
  private long lastRequestCount = 0;
  /** Last request caching count read */
  private long lastRequestCachingCount = 0;
  /** Current window index (next to be updated) */
  private int windowIndex = 0;

  public CacheStats() {
    this(DEFAULT_WINDOW_PERIODS);
  }

  public CacheStats(int numPeriodsInWindow) {
    this.numPeriodsInWindow = numPeriodsInWindow;
    this.hitCounts = initializeZeros(numPeriodsInWindow);
    this.hitCachingCounts = initializeZeros(numPeriodsInWindow);
    this.requestCounts = initializeZeros(numPeriodsInWindow);
    this.requestCachingCounts = initializeZeros(numPeriodsInWindow);
  }

  public void miss(boolean caching) {
    missCount.incrementAndGet();
    if (caching) missCachingCount.incrementAndGet();
  }

  @Override
  public String toString() {
    return "hitCount=" + getHitCount() + ", hitCachingCount=" + getHitCachingCount() +
      ", missCount=" + getMissCount() + ", missCachingCount=" + getMissCachingCount() +
      ", evictionCount=" + getEvictionCount() +
      ", evictedBlockCount=" + getEvictedCount();
  }

  public void miss(boolean caching, BlockType type) {
    missCount.incrementAndGet();
    if (caching) missCachingCount.incrementAndGet();
    if (type == null) {
      return;
    }
    switch (type) {
      case DATA:
      case ENCODED_DATA:
        dataMissCount.incrementAndGet();
        break;
      case LEAF_INDEX:
        leafIndexMissCount.incrementAndGet();
        break;
      case BLOOM_CHUNK:
        bloomChunkMissCount.incrementAndGet();
        break;
      case META:
        metaMissCount.incrementAndGet();
        break;
      case INTERMEDIATE_INDEX:
        intermediateIndexMissCount.incrementAndGet();
        break;
      case ROOT_INDEX:
        rootIndexMissCount.incrementAndGet();
        break;
      case FILE_INFO:
        fileInfoMissCount.incrementAndGet();
        break;
      case GENERAL_BLOOM_META:
        generalBloomMetaMissCount.incrementAndGet();
        break;
      case DELETE_FAMILY_BLOOM_META:
        deleteFamilyBloomMissCount.incrementAndGet();
        break;
      case TRAILER:
        trailerMissCount.incrementAndGet();
        break;
      default:
        // If there's a new type that's fine
        // Ignore it for now. This is metrics don't exception.
        break;
    }
  }

  public void hit(boolean caching, BlockType type) {
    hitCount.incrementAndGet();
    if (caching) hitCachingCount.incrementAndGet();

    if (type == null) {
      return;
    }
    switch (type) {
      case DATA:
      case ENCODED_DATA:
        dataHitCount.incrementAndGet();
        break;
      case LEAF_INDEX:
        leafIndexHitCount.incrementAndGet();
        break;
      case BLOOM_CHUNK:
        bloomChunkHitCount.incrementAndGet();
        break;
      case META:
        metaHitCount.incrementAndGet();
        break;
      case INTERMEDIATE_INDEX:
        intermediateIndexHitCount.incrementAndGet();
        break;
      case ROOT_INDEX:
        rootIndexHitCount.incrementAndGet();
        break;
      case FILE_INFO:
        fileInfoHitCount.incrementAndGet();
        break;
      case GENERAL_BLOOM_META:
        generalBloomMetaHitCount.incrementAndGet();
        break;
      case DELETE_FAMILY_BLOOM_META:
        deleteFamilyBloomHitCount.incrementAndGet();
        break;
      case TRAILER:
        trailerHitCount.incrementAndGet();
        break;
      default:
        // If there's a new type that's fine
        // Ignore it for now. This is metrics don't exception.
        break;
    }
  }

  public void evict() {
    evictionCount.incrementAndGet();
  }

  public void evicted() {
    evictedBlockCount.incrementAndGet();
  }

  public long failInsert() {
    return failedInserts.incrementAndGet();
  }


  // All of the counts of misses and hits.
  public long getDataMissCount() {
    return dataMissCount.get();
  }

  public long getLeafIndexMissCount() {
    return leafIndexMissCount.get();
  }

  public long getBloomChunkMissCount() {
    return bloomChunkMissCount.get();
  }

  public long getMetaMissCount() {
    return metaMissCount.get();
  }

  public long getRootIndexMissCount() {
    return rootIndexMissCount.get();
  }

  public long getIntermediateIndexMissCount() {
    return intermediateIndexMissCount.get();
  }

  public long getFileInfoMissCount() {
    return fileInfoMissCount.get();
  }

  public long getGeneralBloomMetaMissCount() {
    return generalBloomMetaMissCount.get();
  }

  public long getDeleteFamilyBloomMissCount() {
    return deleteFamilyBloomMissCount.get();
  }

  public long getTrailerMissCount() {
    return trailerMissCount.get();
  }

  public long getDataHitCount() {
    return dataHitCount.get();
  }

  public long getLeafIndexHitCount() {
    return leafIndexHitCount.get();
  }

  public long getBloomChunkHitCount() {
    return bloomChunkHitCount.get();
  }

  public long getMetaHitCount() {
    return metaHitCount.get();
  }

  public long getRootIndexHitCount() {
    return rootIndexHitCount.get();
  }

  public long getIntermediateIndexHitCount() {
    return intermediateIndexHitCount.get();
  }

  public long getFileInfoHitCount() {
    return fileInfoHitCount.get();
  }

  public long getGeneralBloomMetaHitCount() {
    return generalBloomMetaHitCount.get();
  }

  public long getDeleteFamilyBloomHitCount() {
    return deleteFamilyBloomHitCount.get();
  }

  public long getTrailerHitCount() {
    return trailerHitCount.get();
  }

  public long getRequestCount() {
    return getHitCount() + getMissCount();
  }

  public long getRequestCachingCount() {
    return getHitCachingCount() + getMissCachingCount();
  }

  public long getMissCount() {
    return missCount.get();
  }

  public long getMissCachingCount() {
    return missCachingCount.get();
  }

  public long getHitCount() {
    return hitCount.get();
  }

  public long getHitCachingCount() {
    return hitCachingCount.get();
  }

  public long getEvictionCount() {
    return evictionCount.get();
  }

  public long getEvictedCount() {
    return evictedBlockCount.get();
  }

  public double getHitRatio() {
    return ((float)getHitCount()/(float)getRequestCount());
  }

  public double getHitCachingRatio() {
    return ((float)getHitCachingCount()/(float)getRequestCachingCount());
  }

  public double getMissRatio() {
    return ((float)getMissCount()/(float)getRequestCount());
  }

  public double getMissCachingRatio() {
    return ((float)getMissCachingCount()/(float)getRequestCachingCount());
  }

  public double evictedPerEviction() {
    return ((float)getEvictedCount()/(float)getEvictionCount());
  }

  public long getFailedInserts() {
    return failedInserts.get();
  }

  public void rollMetricsPeriod() {
    hitCounts[windowIndex] = getHitCount() - lastHitCount;
    lastHitCount = getHitCount();
    hitCachingCounts[windowIndex] =
      getHitCachingCount() - lastHitCachingCount;
    lastHitCachingCount = getHitCachingCount();
    requestCounts[windowIndex] = getRequestCount() - lastRequestCount;
    lastRequestCount = getRequestCount();
    requestCachingCounts[windowIndex] =
      getRequestCachingCount() - lastRequestCachingCount;
    lastRequestCachingCount = getRequestCachingCount();
    windowIndex = (windowIndex + 1) % numPeriodsInWindow;
  }

  public long getSumHitCountsPastNPeriods() {
    return sum(hitCounts);
  }

  public long getSumRequestCountsPastNPeriods() {
    return sum(requestCounts);
  }

  public long getSumHitCachingCountsPastNPeriods() {
    return sum(hitCachingCounts);
  }

  public long getSumRequestCachingCountsPastNPeriods() {
    return sum(requestCachingCounts);
  }

  public double getHitRatioPastNPeriods() {
    double ratio = ((double)sum(hitCounts)/(double)sum(requestCounts));
    return Double.isNaN(ratio) ? 0 : ratio;
  }

  public double getHitCachingRatioPastNPeriods() {
    double ratio =
      ((double)sum(hitCachingCounts)/(double)sum(requestCachingCounts));
    return Double.isNaN(ratio) ? 0 : ratio;
  }

  private static long sum(long [] counts) {
    long sum = 0;
    for (long count : counts) sum += count;
    return sum;
  }

  private static long [] initializeZeros(int n) {
    long [] zeros = new long [n];
    for (int i=0; i<n; i++) {
      zeros[i] = 0L;
    }
    return zeros;
  }
}
