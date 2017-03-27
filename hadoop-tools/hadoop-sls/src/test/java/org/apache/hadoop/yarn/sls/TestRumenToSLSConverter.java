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

package org.apache.hadoop.yarn.sls;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.Map;

public class TestRumenToSLSConverter {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

  @Test
  public void testRumenToSlsConverter() throws Exception {
    File outputfolder = temporaryFolder.newFolder("output");
    File jobsFile = new File(outputfolder,"sls-jobs.json");
    File nodesFile = new File(outputfolder, "sls-nodes.json");
    String args[] = {"-input", "src/main/data/1job-rumen-with-one-crashed-attempt.json",
            "-outputJobs", jobsFile.getAbsolutePath(),
            "-outputNodes", nodesFile.getAbsolutePath()};
    RumenToSLSConverter.main(args);
    Assert.assertTrue(jobsFile.exists());
    Assert.assertTrue(nodesFile.exists());
    JsonFactory jsonF = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper();
    List<Map> jobs = mapper.readValues(jsonF.createParser(jobsFile), Map.class).readAll();
    Assert.assertEquals(1, jobs.size());
    Assert.assertEquals(70, ((List)jobs.get(0).get("job.tasks")).size());
    Assert.assertEquals(8, mapper.readValues(jsonF.createParser(nodesFile), Map.class).readAll().size());
  }
}
