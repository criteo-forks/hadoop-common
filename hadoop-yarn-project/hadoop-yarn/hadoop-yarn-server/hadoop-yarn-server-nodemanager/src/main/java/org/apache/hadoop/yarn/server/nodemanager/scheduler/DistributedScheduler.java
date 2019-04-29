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

package org.apache.hadoop.yarn.server.nodemanager.scheduler;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterDistributedSchedulingAMResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AMRMProxyApplicationContext;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AbstractRequestInterceptor;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>The DistributedScheduler runs on the NodeManager and is modeled as an
 * <code>AMRMProxy</code> request interceptor. It is responsible for the
 * following:</p>
 * <ul>
 *   <li>Intercept <code>ApplicationMasterProtocol</code> calls and unwrap the
 *   response objects to extract instructions from the
 *   <code>ClusterMonitor</code> running on the ResourceManager to aid in making
 *   distributed scheduling decisions.</li>
 * </ul>
 */
public final class DistributedScheduler extends AbstractRequestInterceptor {

  private static final Logger LOG = LoggerFactory
      .getLogger(DistributedScheduler.class);

  private final static RecordFactory RECORD_FACTORY =
      RecordFactoryProvider.getRecordFactory(null);

  // Mapping of NodeId to NodeTokens. Populated either from RM response or
  // generated locally if required.
  private Map<NodeId, NMToken> nodeTokens = new HashMap<>();
  private ApplicationAttemptId applicationAttemptId;
  private NMTokenSecretManagerInNM nmSecretManager;
  private String appSubmitter;
  private long rmIdentifier;

  public void init(AMRMProxyApplicationContext applicationContext) {
    super.init(applicationContext);
    initLocal(applicationContext.getNMCotext().getNodeStatusUpdater()
        .getRMIdentifier(),
        applicationContext.getApplicationAttemptId(),
        applicationContext.getNMCotext().getNMTokenSecretManager(),
        applicationContext.getUser());
  }

  @VisibleForTesting
  void initLocal(long rmId, ApplicationAttemptId appAttemptId,
                 NMTokenSecretManagerInNM nmSecretManager, String appSubmitter) {
    this.rmIdentifier = rmId;
    this.applicationAttemptId = appAttemptId;
    this.nmSecretManager = nmSecretManager;
    this.appSubmitter = appSubmitter;
  }

  /**
   * Route register call to the corresponding distributed scheduling method viz.
   * registerApplicationMasterForDistributedScheduling, and return response to
   * the caller after stripping away Distributed Scheduling information.
   *
   * @param request
   *          registration request
   * @return Allocate Response
   * @throws YarnException YarnException
   * @throws IOException IOException
   */
  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster
      (RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    return registerApplicationMasterForDistributedScheduling(request)
        .getRegisterResponse();
  }

  /**
   * Route allocate call to the allocateForDistributedScheduling method and
   * return response to the caller after stripping away Distributed Scheduling
   * information.
   *
   * @param request
   *          allocation request
   * @return Allocate Response
   * @throws YarnException YarnException
   * @throws IOException IOException
   */
  @Override
  public AllocateResponse allocate(AllocateRequest request) throws
      YarnException, IOException {
    DistributedSchedulingAllocateRequest distRequest = RECORD_FACTORY
        .newRecordInstance(DistributedSchedulingAllocateRequest.class);
    distRequest.setAllocateRequest(request);
    return allocateForDistributedScheduling(distRequest).getAllocateResponse();
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster
      (FinishApplicationMasterRequest request) throws YarnException,
      IOException {
    return getNextInterceptor().finishApplicationMaster(request);
  }

  /**
   * Adds all the newly allocated Containers to the allocate Response.
   * Additionally, in case the NMToken for one of the nodes does not exist, it
   * generates one and adds it to the response.
   */
  private void updateAllocateResponse(AllocateResponse response,
                                      List<NMToken> nmTokens, List<Container> allocatedContainers) {
    List<NMToken> newTokens = new ArrayList<>();
    if (allocatedContainers.size() > 0) {
      response.getAllocatedContainers().addAll(allocatedContainers);
      for (Container alloc : allocatedContainers) {
        if (!nodeTokens.containsKey(alloc.getNodeId())) {
          newTokens.add(nmSecretManager.generateNMToken(appSubmitter, alloc));
        }
      }
      List<NMToken> retTokens = new ArrayList<>(nmTokens);
      retTokens.addAll(newTokens);
      response.setNMTokens(retTokens);
    }
  }

  @Override
  public RegisterDistributedSchedulingAMResponse
      registerApplicationMasterForDistributedScheduling(
          RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    LOG.info("Forwarding registration request to the" +
        "Distributed Scheduler Service on YARN RM");
    RegisterDistributedSchedulingAMResponse dsResp = getNextInterceptor()
        .registerApplicationMasterForDistributedScheduling(request);
    return dsResp;
  }

  @Override
  public DistributedSchedulingAllocateResponse allocateForDistributedScheduling(
      DistributedSchedulingAllocateRequest request)
      throws YarnException, IOException {

    //request.setAllocatedContainers(new ArrayList<Container>());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Forwarding allocate request to the" +
          "Distributed Scheduler Service on YARN RM");
    }

    DistributedSchedulingAllocateResponse dsResp =
        getNextInterceptor().allocateForDistributedScheduling(request);

    // Update host to nodeId mapping
    List<NMToken> nmTokens = dsResp.getAllocateResponse().getNMTokens();
    for (NMToken nmToken : nmTokens) {
      nodeTokens.put(nmToken.getNodeId(), nmToken);
    }

    return dsResp;
  }
}
