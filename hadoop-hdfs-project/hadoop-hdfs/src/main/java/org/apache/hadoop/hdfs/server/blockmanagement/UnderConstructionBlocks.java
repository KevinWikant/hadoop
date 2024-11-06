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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The BlockManager will not add an Under Construction
 * block to the DatanodeDescriptor StorageInfos until
 * the block is fully committed & finalized.
 * The UC block replicas are instead tracked here
 * for the DatanodeAdminManager to use.
 * Note that this is tracked in-memory only, as such
 * some Under Construction blocks may be missed under
 * scenarios where Namenode is restarted.
 **/
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

    Block getBlock() {
      return block;
    }

    DatanodeDescriptor getDatanode() {
      return dn;
    }

    boolean shouldLogWarning() {
      if (Instant.now().isBefore(nextWarnLog)) {
        return false;
      }
      nextWarnLog = Instant.now().plus(LONG_UNDER_CONSTRUCTION_BLOCK_WARN_INTERVAL);
      return true;
    }

    Duration getDurationSinceReporting() {
      return Duration.between(firstReportedTime, Instant.now());
    }

    @Override
    public String toString() {
      return String.format("ReportedBlockInfo [block=%s, dn=%s]", block, dn);
    }
  }

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
   * Remove an Under Construction block replica.
   * This method is called when an Under Construction block replica
   * transitions from UC state to states like: finalized/complete,
   * corrupt, invalidated, and deleted.
   */
  void removeUcBlock(DatanodeDescriptor reportingNode, Block reportedBlock) {
    if (!enabled) {
      return;
    }
    if (reportingNode == null || reportedBlock == null) {
      LOG.warn("Unexpected null input {} , {}", reportingNode, reportedBlock);
    }
    try {
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
        replicasByBlockId.remove(reportedBlock);
        LOG.debug("UC block {} not found on {}, total is [replicas={} / blocks={}]",
                reportedBlock, reportingNode, count, replicasByBlockId.size());
      } else {
        removeUcBlockFromSet(reportingNode, reportedBlock, replicas);
      }
    } catch (Exception e) {
      // Observed during testing that exception thrown here
      // are caught & never logged
      LOG.error("Remove UnderConstruction block {} {} failed",
          reportedBlock, reportingNode, e);
    }
  }

  private void removeUcBlockFromSet(DatanodeDescriptor reportingNode,
                                    Block reportedBlock,
                                    Set<BlockReplica> storedReplicasForBlock) {
    final List<BlockReplica> storedBlocks = storedReplicasForBlock.stream()
            .filter(replica -> reportingNode.equals(replica.getDatanode())
                && reportedBlock.getGenerationStamp() >= replica.getBlock().getGenerationStamp())
            .collect(Collectors.toList());
    storedReplicasForBlock.removeIf(replica -> reportingNode.equals(replica.getDatanode())
        && reportedBlock.getGenerationStamp() >= replica.getBlock().getGenerationStamp());
    if (storedReplicasForBlock.isEmpty()) {
      replicasByBlockId.remove(reportedBlock);
    }
    final String storedBlockString = storedBlocks.stream()
            .map(br -> br.getBlock().toString())
            .collect(Collectors.joining(","));
    if (storedBlocks.size() > 1) {
      LOG.warn("Removed multiple UC block [{}->{}] from {}, total is [replicas={} / blocks={}]",
              storedBlockString, reportedBlock, reportingNode, count, replicasByBlockId.size());
    } else if (storedBlocks.size() == 1) {
      count--;
      LOG.debug("Removed UC block [{}->{}] from {}, new total is [replicas={} / blocks={}]",
              storedBlockString, reportedBlock, reportingNode, count, replicasByBlockId.size());
    } else {
      // Not found on specific datanode
      LOG.debug("UC block {} not found on {}, total is [replicas={} / blocks={}]",
              reportedBlock, reportingNode, count, replicasByBlockId.size());
    }
  }

  /**
   * If the datanode goes DEAD, the Namenode makes an assumption that
   * any write operations have failed.
   */
  void removeAllUcBlocksForDatanode(DatanodeDescriptor reportingNode) {
    if (!enabled) {
      return;
    }
    if (reportingNode == null) {
      LOG.warn("Unexpected null input {}", reportingNode);
    }
    try {
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
      LOG.error("Remove all UnderConstruction block failed for {}",
          reportingNode, e);
    }
  }

  /**
   * Add an Under Construction block replicas for a given block.
   */
  void addUcBlock(DatanodeDescriptor reportingNode, Block reportedBlock) {
    if (!enabled) {
      return;
    }
    if (reportingNode == null || reportedBlock == null) {
      LOG.warn("Unexpected null input {} , {}", reportingNode, reportedBlock);
    }
    try {
      Set<BlockReplica> storedReplicasForBlock;
      if (BlockIdManager.isStripedBlockID(reportedBlock.getBlockId())) {
        Block blkId = new Block(BlockIdManager.convertToStripedID(reportedBlock
                .getBlockId()));
        storedReplicasForBlock = getBlockReplicas(blkId);
      } else {
        reportedBlock = new Block(reportedBlock);
        storedReplicasForBlock = getBlockReplicas(reportedBlock);
      }
      addUcBlockToSet(reportingNode, reportedBlock, storedReplicasForBlock);
    } catch (Exception e) {
      // Observed during testing that exception thrown here
      // are caught & never logged
      LOG.error("Add UnderConstruction block {} on {} failed",
          reportedBlock, reportingNode, e);
    }
  }

  private void addUcBlockToSet(DatanodeDescriptor reportingNode,
                               Block reportedBlock,
                               Set<BlockReplica> storedReplicasForBlock) {
    List<BlockReplica> storedBlocks = storedReplicasForBlock.stream()
            .filter(replica -> reportingNode.equals(replica.getDatanode()))
            .collect(Collectors.toList());
    final String storedBlockString = storedBlocks.stream()
            .map(br -> br.getBlock().toString())
            .collect(Collectors.joining(","));
    if (!storedBlocks.isEmpty()) {
      if (storedBlocks.size() > 1) {
        LOG.warn("UC block [{}->{}] multiple found on {}, total is [replicas={} / blocks={}]",
                storedBlockString, reportedBlock, reportingNode, count, replicasByBlockId.size());
      } else {
        LOG.debug("UC block [{}->{}] already found on {}, total is [replicas={} / blocks={}]",
                storedBlockString, reportedBlock, reportingNode, count, replicasByBlockId.size());
      }
      // Remove any replicas with older/stale GenerationStamp
      storedReplicasForBlock.removeIf(replica -> replica.getDatanode().equals(reportingNode)
          && replica.getBlock().getGenerationStamp() < reportedBlock.getGenerationStamp());
    }
    if (storedReplicasForBlock.stream().noneMatch(replica ->
            reportingNode.equals(replica.getDatanode()))) {
      storedReplicasForBlock.add(new BlockReplica(new Block(reportedBlock), reportingNode));
      count++;
      LOG.debug("Add UC block {} to {}, new total is [replicas={} / blocks={}]",
              reportedBlock, reportingNode, count, replicasByBlockId.size());
    }
  }

  private Set<BlockReplica> getBlockReplicas(Block block) {
    Set<BlockReplica> replicas = replicasByBlockId.get(block);
    if (replicas == null) {
      replicas = new HashSet<>();
      replicasByBlockId.put(block, replicas);
    }
    return replicas;
  }

  public Map<DatanodeDescriptor, List<Block>> getUnderConstructionBlocksByDatanode() {
    if (!enabled) {
      return Maps.newHashMap();
    }
    // Each block can have a ReportedBlockInfo for each block replica.
    // Below is counting all the block replicas for all the open blocks.
    return replicasByBlockId.values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.groupingBy(BlockReplica::getDatanode,
            Collectors.mapping(BlockReplica::getBlock, Collectors.toList())));
  }

  public void logWarningForLongUnderConstructionBlocks() {
    if (!enabled) {
      return;
    }
    if (Instant.now().isBefore(nextWarnLogCheck)) {
      return;
    }
    nextWarnLogCheck = Instant.now().plus(LONG_UNDER_CONSTRUCTION_BLOCK_CHECK_INTERVAL);
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