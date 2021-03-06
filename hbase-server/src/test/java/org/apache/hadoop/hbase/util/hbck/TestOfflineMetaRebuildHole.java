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
package org.apache.hadoop.hbase.util.hbck;

import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.assertErrors;
import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.doFsck;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This builds a table, removes info from meta, and then fails when attempting
 * to rebuild meta.
 */
@Category(MediumTests.class)
public class TestOfflineMetaRebuildHole extends OfflineMetaRebuildTestCore {

  @Test(timeout = 120000)
  public void testMetaRebuildHoleFail() throws Exception {
    // Fully remove a meta entry and hdfs region
    byte[] startKey = splits[1];
    byte[] endKey = splits[2];
    deleteRegion(conf, htbl, startKey, endKey);

    wipeOutMeta();

    // is meta really messed up?
    assertEquals(1, scanMeta());
    assertErrors(doFsck(conf, false), new ERROR_CODE[] {
        ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
        ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
        ERROR_CODE.NOT_IN_META_OR_DEPLOYED});
    // Note, would like to check # of tables, but this takes a while to time
    // out.

    // shutdown the minicluster
    TEST_UTIL.shutdownMiniHBaseCluster();
    TEST_UTIL.shutdownMiniZKCluster();

    // attempt to rebuild meta table from scratch
    HBaseFsck fsck = new HBaseFsck(conf);
    assertFalse(fsck.rebuildMeta(false));

    // bring up the minicluster
    TEST_UTIL.startMiniZKCluster(); // tables seem enabled by default
    TEST_UTIL.restartHBaseCluster(3);

    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL);

    LOG.info("Waiting for no more RIT");
    ZKAssign.blockUntilNoRIT(zkw);
    LOG.info("No more RIT in ZK, now doing final test verification");
    int tries = 60;
    while(TEST_UTIL.getHBaseCluster()
        .getMaster().getAssignmentManager().getRegionStates().getRegionsInTransition().size() > 0 &&
        tries-- > 0) {
      LOG.info("Waiting for RIT: "+TEST_UTIL.getHBaseCluster()
              .getMaster().getAssignmentManager().getRegionStates().getRegionsInTransition());
      Thread.sleep(1000);
    }

    // Meta still messed up.
    assertEquals(1, scanMeta());
    HTableDescriptor[] htbls = TEST_UTIL.getHBaseAdmin().listTables();
    LOG.info("Tables present after restart: " + Arrays.toString(htbls));

    // After HBASE-451 HBaseAdmin.listTables() gets table descriptors from FS,
    // so the table is still present and this should be 1.
    assertEquals(1, htbls.length);
    assertErrors(doFsck(conf, false), new ERROR_CODE[] {
        ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
        ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
        ERROR_CODE.NOT_IN_META_OR_DEPLOYED});
  }

}

