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
package org.apache.hadoop.tools.rumen;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class TestHistoryLogsComparator {

  @Test
  public void testCompareTwoCommonFile(){
    FileStatus file1 = new FileStatus();
    file1.setPath(new Path("/dir1/file1"));
    FileStatus file2 = new FileStatus();
    file2.setPath(new Path("/dir1/file2"));
    FileStatus file3 = new FileStatus();
    file3.setPath(new Path("/dir2/file1"));

    HistoryLogsComparator comparator = new HistoryLogsComparator();

    Assert.assertTrue(comparator.compare(file1, file2) <0);
    Assert.assertTrue(comparator.compare(file2, file1) >0);
    Assert.assertTrue(comparator.compare(file3, file2) <0);
    Assert.assertTrue(comparator.compare(file2, file3) >0);
    Assert.assertTrue(comparator.compare(file1, file3) == 0);
    Assert.assertTrue(comparator.compare(file3, file1) == 0);
  }

  @Test
  public void testCompareOnFileForTheSameJobShouldGiveConfFileAsLowerFile(){
    FileStatus file1 = new FileStatus();
    file1.setPath(new Path("/dir1/job_1488896259152_410442_conf.xml"));
    FileStatus file2 = new FileStatus();
    file2.setPath(new Path("/dir1/job_1488896259152_410442-description.jhist"));

    HistoryLogsComparator comparator = new HistoryLogsComparator();

    Assert.assertTrue(comparator.compare(file1, file2) <0);
    Assert.assertTrue(comparator.compare(file2, file1) >0);
  }

}
