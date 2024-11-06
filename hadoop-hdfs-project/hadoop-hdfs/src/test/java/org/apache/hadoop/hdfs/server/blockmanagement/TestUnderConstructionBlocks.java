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
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Unit test the methods of "UnderConstructionBlocks" class.
 */
public class TestUnderConstructionBlocks {
  private static final Logger LOG = LoggerFactory.getLogger(TestUnderConstructionBlocks.class);

  private static final DatanodeDescriptor DATANODE1 = new DatanodeDescriptor(
      new DatanodeID("10.0.0.1", "host1", "UUID1", 8000, 8001, 8002, 8003));
  private static final DatanodeDescriptor DATANODE2 = new DatanodeDescriptor(
          new DatanodeID("10.0.0.2", "host2", "UUID2", 8000, 8001, 8002, 8003));

  private static final Block BLOCK1 = new Block(123L, 1000L, 1001L);
  private static final Block BLOCK1_V2 = new Block(123L, 1000L, 1002L);
  private static final Block BLOCK1_V3 = new Block(123L, 1000L, 1003L);
  private static final Block BLOCK2 = new Block(456L, 1000L, 1004L);

  @Test
  public void testFeatureDisabled() {
    // Setup
    Configuration conf = Mockito.mock(Configuration.class);
    Mockito.when(conf.getBoolean(DFSConfigKeys.DFS_DECOMMISSION_TRACK_UNDER_CONSTRUCTION_BLOCKS,
            DFSConfigKeys.DFS_DECOMMISSION_TRACK_UNDER_CONSTRUCTION_BLOCKS_DEFAULT))
            .thenReturn(false);
    final UnderConstructionBlocks ucBlocks = new UnderConstructionBlocks(conf);
    // Validate all operations are no-ops
    Assert.assertEquals(0, ucBlocks.getUnderConstructionBlocksByDatanode().size());
    ucBlocks.addUcBlock(DATANODE1, BLOCK1);
    ucBlocks.addUcBlock(DATANODE2, BLOCK2);
    Assert.assertEquals(0, ucBlocks.getUnderConstructionBlocksByDatanode().size());
    ucBlocks.removeUcBlock(DATANODE2, BLOCK2);
    Assert.assertEquals(0, ucBlocks.getUnderConstructionBlocksByDatanode().size());
    ucBlocks.removeAllUcBlocksForDatanode(DATANODE1);
    Assert.assertEquals(0, ucBlocks.getUnderConstructionBlocksByDatanode().size());
  }

  @Test
  public void testAddAndRemoveUcBlocks() {
    // Setup
    Configuration conf = Mockito.mock(Configuration.class);
    Mockito.when(conf.getBoolean(DFSConfigKeys.DFS_DECOMMISSION_TRACK_UNDER_CONSTRUCTION_BLOCKS,
            DFSConfigKeys.DFS_DECOMMISSION_TRACK_UNDER_CONSTRUCTION_BLOCKS_DEFAULT))
            .thenReturn(true);
    final UnderConstructionBlocks ucBlocks = new UnderConstructionBlocks(conf);
    Assert.assertTrue(ucBlocks.getUnderConstructionBlocksByDatanode().isEmpty());
    // Test Add
    ucBlocks.addUcBlock(DATANODE1, BLOCK1);
    ucBlocks.addUcBlock(DATANODE1, BLOCK2);
    ucBlocks.addUcBlock(DATANODE2, BLOCK2);
    Map<DatanodeDescriptor, List<Block>> byDatanode =
        ucBlocks.getUnderConstructionBlocksByDatanode();
    Assert.assertEquals(2, byDatanode.size());
    Assert.assertEquals(2, byDatanode.get(DATANODE1).size());
    Assert.assertTrue(byDatanode.get(DATANODE1).stream().anyMatch(b -> b.equals(BLOCK1)));
    Assert.assertTrue(byDatanode.get(DATANODE1).stream().anyMatch(b -> b.equals(BLOCK2)));
    Assert.assertEquals(1, byDatanode.get(DATANODE2).size());
    Assert.assertEquals(BLOCK2, byDatanode.get(DATANODE2).get(0));
    // Test Remove
    ucBlocks.removeUcBlock(DATANODE1, BLOCK2);
    ucBlocks.removeUcBlock(DATANODE2, BLOCK2);
    byDatanode = ucBlocks.getUnderConstructionBlocksByDatanode();
    Assert.assertEquals(1, byDatanode.size());
    Assert.assertEquals(1, byDatanode.get(DATANODE1).size());
    Assert.assertEquals(BLOCK1, byDatanode.get(DATANODE1).get(0));
    Assert.assertNull(byDatanode.get(DATANODE2));
    // Test Add (with duplicate)
    ucBlocks.addUcBlock(DATANODE1, BLOCK1);
    ucBlocks.addUcBlock(DATANODE1, BLOCK1);
    ucBlocks.addUcBlock(DATANODE2, BLOCK1);
    ucBlocks.addUcBlock(DATANODE2, BLOCK1);
    byDatanode = ucBlocks.getUnderConstructionBlocksByDatanode();
    Assert.assertEquals(2, byDatanode.size());
    Assert.assertEquals(1, byDatanode.get(DATANODE1).size());
    Assert.assertEquals(BLOCK1, byDatanode.get(DATANODE1).get(0));
    Assert.assertEquals(1, byDatanode.get(DATANODE2).size());
    Assert.assertEquals(BLOCK1, byDatanode.get(DATANODE2).get(0));
    // Test Remove All
    ucBlocks.removeAllUcBlocksForDatanode(DATANODE1);
    byDatanode = ucBlocks.getUnderConstructionBlocksByDatanode();
    Assert.assertEquals(1, byDatanode.size());
    ucBlocks.removeAllUcBlocksForDatanode(DATANODE2);
    byDatanode = ucBlocks.getUnderConstructionBlocksByDatanode();
    Assert.assertEquals(0, byDatanode.size());
  }

  @Test
  public void testRemoveUcBlocksWithOldGenerationStamp() {
    // Setup
    Configuration conf = Mockito.mock(Configuration.class);
    Mockito.when(conf.getBoolean(DFSConfigKeys.DFS_DECOMMISSION_TRACK_UNDER_CONSTRUCTION_BLOCKS,
            DFSConfigKeys.DFS_DECOMMISSION_TRACK_UNDER_CONSTRUCTION_BLOCKS_DEFAULT))
            .thenReturn(true);
    final UnderConstructionBlocks ucBlocks = new UnderConstructionBlocks(conf);
    Assert.assertTrue(ucBlocks.getUnderConstructionBlocksByDatanode().isEmpty());
    // Add with v2 Genstamp
    ucBlocks.addUcBlock(DATANODE2, BLOCK1_V2);
    Map<DatanodeDescriptor, List<Block>> byDatanode =
        ucBlocks.getUnderConstructionBlocksByDatanode();
    Assert.assertEquals(1, byDatanode.size());
    Assert.assertEquals(1, byDatanode.get(DATANODE2).size());
    Assert.assertEquals(BLOCK1_V2, byDatanode.get(DATANODE2).get(0));
    Assert.assertEquals(BLOCK1_V2.getGenerationStamp(),
        byDatanode.get(DATANODE2).get(0).getGenerationStamp());
    // Remove with older Genstamp (should have no effect)
    ucBlocks.removeUcBlock(DATANODE2, BLOCK1);
    byDatanode = ucBlocks.getUnderConstructionBlocksByDatanode();
    Assert.assertEquals(1, byDatanode.size());
    Assert.assertEquals(1, byDatanode.get(DATANODE2).size());
    Assert.assertEquals(BLOCK1_V2, byDatanode.get(DATANODE2).get(0));
    Assert.assertEquals(BLOCK1_V2.getGenerationStamp(),
        byDatanode.get(DATANODE2).get(0).getGenerationStamp());
    // Remove with newer Genstamp (should have effect)
    ucBlocks.removeUcBlock(DATANODE2, BLOCK1_V3);
    byDatanode = ucBlocks.getUnderConstructionBlocksByDatanode();
    Assert.assertEquals(0, byDatanode.size());
  }

  @Test
  public void testAddUcBlocksWithOldGenerationStamp() {
    // Setup
    Configuration conf = Mockito.mock(Configuration.class);
    Mockito.when(conf.getBoolean(DFSConfigKeys.DFS_DECOMMISSION_TRACK_UNDER_CONSTRUCTION_BLOCKS,
            DFSConfigKeys.DFS_DECOMMISSION_TRACK_UNDER_CONSTRUCTION_BLOCKS_DEFAULT))
            .thenReturn(true);
    final UnderConstructionBlocks ucBlocks = new UnderConstructionBlocks(conf);
    Assert.assertTrue(ucBlocks.getUnderConstructionBlocksByDatanode().isEmpty());
    // Add with v2 Genstamp
    ucBlocks.addUcBlock(DATANODE2, BLOCK1_V2);
    Map<DatanodeDescriptor, List<Block>> byDatanode =
        ucBlocks.getUnderConstructionBlocksByDatanode();
    Assert.assertEquals(1, byDatanode.size());
    Assert.assertEquals(1, byDatanode.get(DATANODE2).size());
    Assert.assertEquals(BLOCK1_V2, byDatanode.get(DATANODE2).get(0));
    Assert.assertEquals(BLOCK1_V2.getGenerationStamp(),
            byDatanode.get(DATANODE2).get(0).getGenerationStamp());
    // Add with older Genstamp (should have no effect)
    ucBlocks.addUcBlock(DATANODE2, BLOCK1);
    byDatanode = ucBlocks.getUnderConstructionBlocksByDatanode();
    Assert.assertEquals(1, byDatanode.size());
    Assert.assertEquals(1, byDatanode.get(DATANODE2).size());
    Assert.assertEquals(BLOCK1_V2, byDatanode.get(DATANODE2).get(0));
    Assert.assertEquals(BLOCK1_V2.getGenerationStamp(),
            byDatanode.get(DATANODE2).get(0).getGenerationStamp());
    // Add with newer Genstamp (should cause Genstamp to be updated)
    ucBlocks.addUcBlock(DATANODE2, BLOCK1_V3);
    byDatanode = ucBlocks.getUnderConstructionBlocksByDatanode();
    Assert.assertEquals(1, byDatanode.size());
    Assert.assertEquals(1, byDatanode.get(DATANODE2).size());
    Assert.assertEquals(BLOCK1_V3, byDatanode.get(DATANODE2).get(0));
    Assert.assertEquals(BLOCK1_V3.getGenerationStamp(),
        byDatanode.get(DATANODE2).get(0).getGenerationStamp());
  }

  /**
   * This is the actual sequence of events which occurs when a block
   * has a new Genstamp created.
   * - replica with old Genstamp is marked stale & removed
   * - replica with new Genstamp is then added afterward
   */
  @Test
  public void testGenstampPromotionScenario() {
    // Setup
    Configuration conf = Mockito.mock(Configuration.class);
    Mockito.when(conf.getBoolean(DFSConfigKeys.DFS_DECOMMISSION_TRACK_UNDER_CONSTRUCTION_BLOCKS,
            DFSConfigKeys.DFS_DECOMMISSION_TRACK_UNDER_CONSTRUCTION_BLOCKS_DEFAULT))
            .thenReturn(true);
    final UnderConstructionBlocks ucBlocks = new UnderConstructionBlocks(conf);
    Assert.assertTrue(ucBlocks.getUnderConstructionBlocksByDatanode().isEmpty());
    // Test Add
    ucBlocks.addUcBlock(DATANODE1, BLOCK1);
    ucBlocks.addUcBlock(DATANODE1, BLOCK2);
    ucBlocks.addUcBlock(DATANODE2, BLOCK1);
    Map<DatanodeDescriptor, List<Block>> byDatanode =
        ucBlocks.getUnderConstructionBlocksByDatanode();
    Assert.assertEquals(2, byDatanode.size());
    Assert.assertEquals(2, byDatanode.get(DATANODE1).size());
    Assert.assertTrue(byDatanode.get(DATANODE1).stream().anyMatch(b -> b.equals(BLOCK1)));
    Assert.assertTrue(byDatanode.get(DATANODE1).stream().anyMatch(b -> b.equals(BLOCK2)));
    Assert.assertEquals(1, byDatanode.get(DATANODE2).size());
    Assert.assertEquals(BLOCK1, byDatanode.get(DATANODE2).get(0));
    // Remove stale replica
    ucBlocks.removeUcBlock(DATANODE1, BLOCK1);
    ucBlocks.removeUcBlock(DATANODE2, BLOCK1_V2);
    byDatanode = ucBlocks.getUnderConstructionBlocksByDatanode();
    Assert.assertEquals(1, byDatanode.size());
    Assert.assertEquals(1, byDatanode.get(DATANODE1).size());
    Assert.assertEquals(BLOCK2, byDatanode.get(DATANODE1).get(0));
    // Add v2 Genstamp replicas
    ucBlocks.addUcBlock(DATANODE1, BLOCK1_V2);
    ucBlocks.addUcBlock(DATANODE2, BLOCK1_V2);
    byDatanode = ucBlocks.getUnderConstructionBlocksByDatanode();
    Assert.assertEquals(2, byDatanode.size());
    Assert.assertEquals(2, byDatanode.get(DATANODE1).size());
    Assert.assertTrue(byDatanode.get(DATANODE1).stream().anyMatch(b -> b.equals(BLOCK1_V2)));
    Assert.assertTrue(byDatanode.get(DATANODE1).stream().anyMatch(b -> b.equals(BLOCK2)));
    Assert.assertEquals(1, byDatanode.get(DATANODE2).size());
    Assert.assertEquals(BLOCK1_V2, byDatanode.get(DATANODE2).get(0));
  }
}
