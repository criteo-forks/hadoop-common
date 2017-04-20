/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.sls.mockdns;

import sun.net.spi.nameservice.NameService;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class MockDNSService implements NameService {

  private static final String LOCALHOST = "localhost";
  private static final byte[] LOCAL_IP = new byte[]{0x7f, 0x00, 0x00, 0x01};

  @Override
  public InetAddress[] lookupAllHostAddr(String s) throws UnknownHostException {
    return new InetAddress[]{InetAddress.getByAddress(s, LOCAL_IP)};
  }

  @Override
  public String getHostByAddr(byte[] bytes) throws UnknownHostException {
    return LOCALHOST;
  }
}
