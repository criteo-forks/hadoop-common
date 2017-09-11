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
package org.apache.hadoop.yarn.sls.utils;

import org.junit.Assert;
import org.junit.Test;

public class TestScaleClock {

  @Test
  public void testGetTime() throws Exception {
    ScaleClock clock = new ScaleClock(10L);
    long startReal = System.currentTimeMillis();
    long startSimulation = clock.getTime();
    Thread.sleep(20L);
    long endSimulation = clock.getTime();
    long endReal = System.currentTimeMillis();
    Assert.assertTrue((endSimulation - startSimulation) >= 200L);
    Assert.assertTrue((endSimulation - startSimulation) <= (endReal - startReal) * 10L);
  }

  @Test
  public void testGetTick() throws Exception {
    ScaleClock clock = new ScaleClock(10L);
    long startReal = System.nanoTime();
    long startSimulation = clock.getTick();
    Thread.sleep(20L);
    long endSimulation = clock.getTick();
    long endReal = System.nanoTime();
    Assert.assertTrue((endSimulation - startSimulation) >= 200000L);
    Assert.assertTrue((endSimulation - startSimulation) <= (endReal - startReal) * 10L);
  }
}
