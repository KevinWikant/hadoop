/**
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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The BlockManager will not add an Under Construction
 * block to the DatanodeDescriptor StorageInfos until
 * the block is fully committed and finalized.
 * The Under Construction block replicas are instead tracked
 * here for the DatanodeAdminManager to use.
 *
 * Note that Under Construction is a term in the HDFS code
 * base used to refer to a block replica which is held open
 * by an HDFS client. The Under Construction block replica
 * state is RBW (i.e. replica being written).
 *
 * Also note that this data structure is tracked in-memory only,
 * as such some Under Construction blocks may be missed under
 * scenarios where Namenode is restarted.
 */
public class UnderConstructionBlocks {
  private static final Logger LOG =
          LoggerFactory.getLogger(UnderConstructionBlocks.class);

  // Amount of time to wait in between checking all block replicas
  private static final Duration LONG_UNDER_CONSTRUCTION_BLOCK_CHECK_INTERVAL
          = Duration.ofMinutes(5);
  // Amount of time to wait before logging each individual block replica
  // as warning.
  private static final Duration LONG_UNDER_CONSTRUCTION_BLOCK_WARN_THRESHOLD
      = Duration.ofHours(2);
  private static final Duration LONG_UNDER_CONSTRUCTION_BLOCK_WARN_INTERVAL
      = Duration.ofMinutes(30);

  private final Map<Block, Set<BlockReplica>> replicasByBlockId =
      Maps.newHashMap();
  private final boolean enabled;
  // Total count of Under Construction replicas. The count will match the sum
  // of the sizes of all the sets of BlockReplicas in "replicasByBlockId".
  // The count is stored here to avoid the cost of recomputing the sum
  // each time it is needed.
  private int count = 0;
  // DatanodeAdminMonitor invokes logWarningForLongUnderConstructionBlocks every 30 seconds.
  // To reduce the number of times this method loops through the Under Construction blocks,
  // the interval is limited by LONG_UNDER_CONSTRUCTION_BLOCK_CHECK_INTERVAL.
  private Instant nextWarnLogCheck =
      Instant.now().plus(LONG_UNDER_CONSTRUCTION_BLOCK_CHECK_INTERVAL);

  static class BlockReplica {
    private final Block block;
    private final DatanodeDescriptor dn;
    private final Instant firstReportedTime;
    private Instant nextWarnLog;

    BlockReplica(Block block,
                 DatanodeDescriptor dn) {
      this.block = block;
      this.dn = dn;
      this.firstReportedTime = Instant.now();
      this.nextWarnLog = firstReportedTime.plus(LONG_UNDER_CONSTRUCTION_BLOCK_WARN_THRESHOLD);
    }

    /** @return - block ID for the Under Construction block replica. */
    Block getBlock() {
      return block;
    }

    /** @return - datanode ID for datanode storing the Under Construction block replica. */
    DatanodeDescriptor getDatanode() {
      return dn;
    }

    /**
     * Determines if a warning should be logged based on how long the Under Construction
     * block replica has been in RBW state and when a warning was last logged.
     *
     * @return - boolean indicating if warning should be logged for this block replica.
     */
    boolean shouldLogWarning() {
      if (Instant.now().isBefore(nextWarnLog)) {
        return false;
      }
      nextWarnLog = Instant.now().plus(LONG_UNDER_CONSTRUCTION_BLOCK_WARN_INTERVAL);
      return true;
    }

    /** @return - duration since the block replica was first reported as Under Construction. */
    Duration getDurationSinceReporting() {
      return Duration.between(firstReportedTime, Instant.now());
    }

    @Override
    public String toString() {
      return String.format("ReportedBlockInfo [block=%s, dn=%s]", block, dn);
    }
  }

  /**
   * Initializes the data structure for tracking Under Construction block replicas.
   *
   * @param conf - the Hadoop HDFS configuration keys & values.
   */
  UnderConstructionBlocks(Configuration conf) {
    this.enabled = conf.getBoolean(
        DFSConfigKeys.DFS_DECOMMISSION_TRACK_UNDER_CONSTRUCTION_BLOCKS,
        DFSConfigKeys.DFS_DECOMMISSION_TRACK_UNDER_CONSTRUCTION_BLOCKS_DEFAULT);
    if (enabled) {
      LOG.info("Tracking Under Construction blocks for DatanodeAdminManager");
    } else {
      LOG.debug("DatanodeAdminManager will not track Under Construction blocks");
    }
  }

  /**
   * Stop tracking an Under Construction block replica.
   * This method is called when an Under Construction block replica
   * transitions from Under Construction (a.k.a. RBW) state to
   * states like: finalized/complete, corrupt, invalidated,
   * and deleted.
   *
   * @param reportingNode - datanode which is storing the Under Construction block replica.
   * @param reportedBlock - Under Construction block replica to stop tracking for the datanode.
   */
  void removeUcBlock(DatanodeDescriptor reportingNode, Block reportedBlock) {
    if (!enabled) {
      return;
    }
    if (reportingNode == null || reportedBlock == null) {
      LOG.warn("Remove UnderConstruction block has unexpected null input");
      return;
    }
    try {
      // Extract set of BlockReplicas matching the reportedBlock
      Set<BlockReplica> replicas;
      if (BlockIdManager.isStripedBlockID(reportedBlock.getBlockId())) {
        Block blkId = new Block(BlockIdManager.convertToStripedID(reportedBlock
                .getBlockId()));
        replicas = getBlockReplicas(blkId);
      } else {
        reportedBlock = new Block(reportedBlock);
        replicas = getBlockReplicas(reportedBlock);
      }
      if (replicas.isEmpty()) {
        // If the UC block replica is not stored on any datanodes,
        // remove it from the "replicasByBlockId" map
        replicasByBlockId.remove(reportedBlock);
        LOG.debug("UC block {} not found on {}, total is [replicas={} / blocks={}]",
                reportedBlock, reportingNode, count, replicasByBlockId.size());
      } else {
        // If the UC block replica is stored on some datanodes, then remove
        // the reportingNode from the set if present
        removeUcBlockFromSet(reportingNode, reportedBlock, replicas);
      }
    } catch (Exception e) {
      // Observed during testing that exception thrown here
      // are caught & never logged
      LOG.warn("Remove UnderConstruction block {} {} failed",
          reportedBlock, reportingNode, e);
    }
  }

  /**
   * Private helper method to stop tracking an Under Construction block replica.
   *
   * @param reportingNode - datanode which is storing the Under Construction block replica.
   * @param reportedBlock - Under Construction block replica to stop tracking for the datanode.
   * @param storedReplicasForBlock - list of BlockReplica objects associated with the reportedBlock.
   */
  private void removeUcBlockFromSet(DatanodeDescriptor reportingNode,
                                    Block reportedBlock,
                                    Set<BlockReplica> storedReplicasForBlock) {
    // Extract the set of block replicas for the reportedBlock stored on the reportingNode.
    // This reference is used for validation after the block replica is removed from the set.
    final List<BlockReplica> storedBlocks = storedReplicasForBlock.stream()
            .filter(replica -> reportingNode.equals(replica.getDatanode())
                && reportedBlock.getGenerationStamp() >= replica.getBlock().getGenerationStamp())
            .collect(Collectors.toList());
    // Stop tracking the block replica for the reportedBlock stored on the reportingNode
    storedReplicasForBlock.removeIf(replica -> reportingNode.equals(replica.getDatanode())
        && reportedBlock.getGenerationStamp() >= replica.getBlock().getGenerationStamp());
    if (storedReplicasForBlock.isEmpty()) {
      // If the UC block replica is not stored on any datanodes,
      // remove it from the "replicasByBlockId" map
      replicasByBlockId.remove(reportedBlock);
    }

    // Log appropriate message based on the number of existing replicas
    final String storedBlockString = storedBlocks.stream()
            .map(br -> br.getBlock().toString())
            .collect(Collectors.joining(","));
    if (storedBlocks.size() > 1) {
      // Duplicate block replicas were found for the reportingNode. This should never occur
      // because each UC replica should only have one copy stored in "replicasByBlockId".
      // Log a warning for this unexpected case.
      LOG.warn("Removed multiple UC block [{}->{}] from {}, total is [replicas={} / blocks={}]",
              storedBlockString, reportedBlock, reportingNode, count, replicasByBlockId.size());
    } else if (storedBlocks.size() == 1) {
      // Exactly one replica removed, decrement the total UC replica count & log a debug message
      count--;
      LOG.debug("Removed UC block [{}->{}] from {}, new total is [replicas={} / blocks={}]",
              storedBlockString, reportedBlock, reportingNode, count, replicasByBlockId.size());
    } else {
      // No replicas were found/removed for this block on the specific datanode
      LOG.debug("UC block {} not found on {}, total is [replicas={} / blocks={}]",
              reportedBlock, reportingNode, count, replicasByBlockId.size());
    }
  }

  /**
   * If the datanode goes DEAD, the Namenode makes an assumption that
   * any write operations have failed.
   *
   * @param reportingNode - datanode which no longer has any Under Construction blocks.
   */
  void removeAllUcBlocksForDatanode(DatanodeDescriptor reportingNode) {
    if (!enabled) {
      return;
    }
    if (reportingNode == null) {
      LOG.warn("Remove all UnderConstruction block has unexpected null input");
      return;
    }
    try {
      // Stop tracking all block replicas associated with the datanode
      Set<Block> toRemoveFromMap = new HashSet<>();
      Set<BlockReplica> removedReplicas = new HashSet<>();
      for (Map.Entry<Block, Set<BlockReplica>> entry: replicasByBlockId.entrySet()) {
        final List<BlockReplica> storedBlocks = entry.getValue().stream()
                .filter(replica -> reportingNode.equals(replica.getDatanode()))
                .collect(Collectors.toList());
        entry.getValue().removeIf(replica -> reportingNode.equals(replica.getDatanode()));
        removedReplicas.addAll(storedBlocks);
        count -= storedBlocks.size();
        if (entry.getValue().isEmpty()) {
          toRemoveFromMap.add(entry.getKey());
        }
      }
      // Remove map entries for block replicas which are no longer stored on any datanodes
      for (Block remove: toRemoveFromMap) {
        replicasByBlockId.remove(remove);
      }
      final String removedBlocksString = removedReplicas.stream()
              .map(br -> br.getBlock().toString())
              .collect(Collectors.joining(","));
      LOG.debug("Removed [{}] UC blocks for {}, new total is [replicas={} / blocks={}]",
              removedBlocksString, reportingNode, count, replicasByBlockId.size());
    } catch (Exception e) {
      // Observed during testing that exception thrown here
      // are caught & never logged
      LOG.warn("Remove all UnderConstruction block failed for {}",
          reportingNode, e);
    }
  }

  /**
   * Start tracking an Under Construction block replica.
   * This method is called when a block replica is
   * reported in RBW (i.e. replica being written) state.
   *
   * @param reportingNode - datanode which is storing the Under Construction block replica.
   * @param reportedBlock - Under Construction block replica to start tracking for the datanode.
   */
  void addUcBlock(DatanodeDescriptor reportingNode, Block reportedBlock) {
    if (!enabled) {
      return;
    }
    if (reportingNode == null || reportedBlock == null) {
      LOG.warn("Add UnderConstruction block has unexpected null input");
      return;
    }
    try {
      // Extract set of block replicas matching the reportedBlock
      Set<BlockReplica> storedReplicasForBlock;
      if (BlockIdManager.isStripedBlockID(reportedBlock.getBlockId())) {
        Block blkId = new Block(BlockIdManager.convertToStripedID(reportedBlock
                .getBlockId()));
        storedReplicasForBlock = getBlockReplicas(blkId);
      } else {
        reportedBlock = new Block(reportedBlock);
        storedReplicasForBlock = getBlockReplicas(reportedBlock);
      }

      // Start tracking the new BlockReplica if not already present
      addUcBlockToSet(reportingNode, reportedBlock, storedReplicasForBlock);
    } catch (Exception e) {
      // Observed during testing that exception thrown here
      // are caught & never logged
      LOG.warn("Add UnderConstruction block {} on {} failed",
          reportedBlock, reportingNode, e);
    }
  }

  /**
   * Private helper method to start tracking an Under Construction block replica.
   *
   * @param reportingNode - datanode which is storing the Under Construction block replica.
   * @param reportedBlock - Under Construction block replica to start tracking for the datanode.
   * @param storedReplicasForBlock - list of BlockReplica objects associated with the reportedBlock.
   */
  private void addUcBlockToSet(DatanodeDescriptor reportingNode,
                               Block reportedBlock,
                               Set<BlockReplica> storedReplicasForBlock) {
    // Extract the set of block replicas for the reportedBlock stored on the reportingNode.
    // This reference is used for validations such as checking for duplicate entries.
    List<BlockReplica> storedBlocks = storedReplicasForBlock.stream()
            .filter(replica -> reportingNode.equals(replica.getDatanode()))
            .collect(Collectors.toList());
    final String storedBlockString = storedBlocks.stream()
            .map(br -> br.getBlock().toString())
            .collect(Collectors.joining(","));

    // Log appropriate message based on the number of existing block replicas
    if (!storedBlocks.isEmpty()) {
      if (storedBlocks.size() > 1) {
        // Duplicate block replicas were found for the reportingNode. This should never occur
        // because each UC replica should only have one copy stored in "replicasByBlockId".
        // Log a warning for this unexpected case
        LOG.warn("UC block [{}->{}] multiple found on {}, total is [replicas={} / blocks={}]",
                storedBlockString, reportedBlock, reportingNode, count, replicasByBlockId.size());
      } else {
        // Block replica is already being tracked for the datanode
        LOG.debug("UC block [{}->{}] already found on {}, total is [replicas={} / blocks={}]",
                storedBlockString, reportedBlock, reportingNode, count, replicasByBlockId.size());
      }
      // Remove any replicas with older/stale GenerationStamp. These block replicas with older
      // generation stamp should be replaced by newer block replica version with newer
      // generation stamp
      storedReplicasForBlock.removeIf(replica -> replica.getDatanode().equals(reportingNode)
          && replica.getBlock().getGenerationStamp() < reportedBlock.getGenerationStamp());
    }

    // Add the UC block replica, increment the total UC replica count, & log a debug message
    if (storedReplicasForBlock.stream().noneMatch(replica ->
            reportingNode.equals(replica.getDatanode()))) {
      storedReplicasForBlock.add(new BlockReplica(new Block(reportedBlock), reportingNode));
      count++;
      LOG.debug("Add UC block {} to {}, new total is [replicas={} / blocks={}]",
              reportedBlock, reportingNode, count, replicasByBlockId.size());
    }
  }

  /**
   * Returns all the Under Construction block replicas associated with a given block ID.
   *
   * @param block - block ID to get the Under Construction block replicas for.
   * @return - list of Under Construction BlockReplicas associated with the block ID.
   */
  private Set<BlockReplica> getBlockReplicas(Block block) {
    Set<BlockReplica> replicas = replicasByBlockId.get(block);
    if (replicas == null) {
      replicas = new HashSet<>();
      replicasByBlockId.put(block, replicas);
    }
    return replicas;
  }

 /**
  * Returns all the Under Construction block replicas in an immutable map keyed by datanode.
  *
  * @return - immutable map of Under Construction block replicas.
  */
  public Map<DatanodeDescriptor, List<Block>> getUnderConstructionBlocksByDatanode() {
    if (!enabled) {
      return Maps.newHashMap();
    }
    // Create a map from DatanodeDescriptor to a list of all Under Construction block replicas
    // stored on the associated datanode.
    final Map<DatanodeDescriptor, List<Block>> result = replicasByBlockId.values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.groupingBy(BlockReplica::getDatanode,
            Collectors.mapping(BlockReplica::getBlock, Collectors.toList())));
    // If debug logging is enabled, print the count of Under Construction block replicas
    // for each datanode.
    if (LOG.isDebugEnabled()) {
      String ucBlockCounts = result.entrySet().stream()
              .map(e -> String.format("%s=%d", e.getKey(), e.getValue().size()))
              .collect(Collectors.joining(",", "{", "}"));
      LOG.debug("Under Construction block counts: [{}]", ucBlockCounts);
    }
    // Return the result as an unmodifiable map
    return Collections.unmodifiableMap(result);
  }

  /**
   * Log a warning for each block replica which has been Under Construction for
   * longer than LONG_UNDER_CONSTRUCTION_BLOCK_WARN_THRESHOLD. For each Under
   * Construction block replica, rate limit the frequency of this log to be
   * printed to be once every LONG_UNDER_CONSTRUCTION_BLOCK_WARN_INTERVAL.
   */
  public void logWarningForLongUnderConstructionBlocks() {
    if (!enabled) {
      return;
    }
    // DatanodeAdminMonitor invokes logWarningForLongUnderConstructionBlocks every 30 seconds.
    // To reduce the number of times this method loops through the Under Construction blocks,
    // the interval is limited by LONG_UNDER_CONSTRUCTION_BLOCK_CHECK_INTERVAL.
    if (Instant.now().isBefore(nextWarnLogCheck)) {
      return;
    }
    nextWarnLogCheck = Instant.now().plus(LONG_UNDER_CONSTRUCTION_BLOCK_CHECK_INTERVAL);

    // Log a warning for each Under Construction block replica which meets the conditions.
    Stream<BlockReplica> allReplicas = replicasByBlockId.values()
        .stream().flatMap(Collection::stream);
    allReplicas.forEach(replica -> {
      if (replica.shouldLogWarning()) {
        LOG.warn("Block {} on {} has been UC for {} minutes",
            replica.getBlock(), replica.getDatanode(),
            replica.getDurationSinceReporting().toMinutes());
      }
    });
  }
}