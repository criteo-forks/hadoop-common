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

import java.io.Serializable;
import java.util.Comparator;

public class HistoryLogsComparator implements Comparator<FileStatus>, Serializable {
  /**
   * Compare the history file names, not the full paths.
   * Job history file name format is such that doing lexicographic sort on the
   * history file names should result in the order of jobs' submission times.
   */

  @Override
  public int compare(FileStatus file1, FileStatus file2) {
    String jobId1 = JobHistoryUtils.extractJobID(file1.getPath().getName());
    if (jobId1 != null && jobId1.equals(JobHistoryUtils.extractJobID(file2.getPath().getName()))) {
      if (JobHistoryUtils.isJobConfXml(file1.getPath().getName())) {
        if (!JobHistoryUtils.isJobConfXml(file2.getPath().getName())) {
          return -1;
        }
      } else if (JobHistoryUtils.isJobConfXml(file2.getPath().getName())) {
        return +1;
      }
    }
    return genericCompare(file1, file2);
  }

  private int genericCompare(FileStatus file1, FileStatus file2) {
    return file1.getPath().getName().compareTo(file2.getPath().getName());
  }


}
