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
package org.apache.hadoop.yarn.sls.mockdns;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class TestMockDNSService {

  @Test
  public void testLookupAllHostAddr() throws UnknownHostException {
    MockDNSService dnsService = new MockDNSService();
    InetAddress[] addresses = dnsService.lookupAllHostAddr("www.apache.org");
    Assert.assertNotNull(addresses);
    Assert.assertEquals(1, addresses.length);
    Assert.assertArrayEquals(new byte[]{0x7f, 0x00, 0x00, 0x1}, addresses[0].getAddress());
    Assert.assertEquals("www.apache.org", addresses[0].getHostName());
  }

  @Test
  public void testGetHostByAddr() throws UnknownHostException {
    MockDNSService dnsService = new MockDNSService();
    String host = dnsService.getHostByAddr(new byte[]{88, (byte)198, 26, 2});
    Assert.assertEquals("localhost", host);
  }
}
