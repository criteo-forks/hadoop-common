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

package org.apache.hadoop.yarn.api.records;

import java.text.NumberFormat;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p><code>ApplicationAttemptId</code> denotes the particular <em>attempt</em>
 * of an <code>ApplicationMaster</code> for a given {@link ApplicationId}.</p>
 * 
 * <p>Multiple attempts might be needed to run an application to completion due
 * to temporal failures of the <code>ApplicationMaster</code> such as hardware
 * failures, connectivity issues etc. on the node on which it was scheduled.</p>
 */
@Public
@Stable
public abstract class ApplicationAttemptId implements
    Comparable<ApplicationAttemptId> {

  @Private
  @Unstable
  public static final String appAttemptIdStrPrefix = "appattempt_";

  private static final String APP_ATTEMPT_ID_PREFIX = appAttemptIdStrPrefix
          + '_';

  @Private
  @Unstable
  public static ApplicationAttemptId newInstance(ApplicationId appId,
      int attemptId) {
    ApplicationAttemptId appAttemptId =
        Records.newRecord(ApplicationAttemptId.class);
    appAttemptId.setApplicationId(appId);
    appAttemptId.setAttemptId(attemptId);
    appAttemptId.build();
    return appAttemptId;
  }

  /**
   * Get the <code>ApplicationId</code> of the <code>ApplicationAttempId</code>. 
   * @return <code>ApplicationId</code> of the <code>ApplicationAttempId</code>
   */
  @Public
  @Stable
  public abstract ApplicationId getApplicationId();
  
  @Private
  @Unstable
  protected abstract void setApplicationId(ApplicationId appID);
  
  /**
   * Get the <code>attempt id</code> of the <code>Application</code>.
   * @return <code>attempt id</code> of the <code>Application</code>
   */
  @Public
  @Stable
  public abstract int getAttemptId();
  
  @Private
  @Unstable
  protected abstract void setAttemptId(int attemptId);

  static final ThreadLocal<NumberFormat> attemptIdFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(6);
          return fmt;
        }
      };

  @Override
  public int hashCode() {
    // Generated by eclipse.
    final int prime = 347671;
    int result = 5501;
    ApplicationId appId = getApplicationId();
    result = prime * result +  appId.hashCode();
    result = prime * result + getAttemptId();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ApplicationAttemptId other = (ApplicationAttemptId) obj;
    if (!this.getApplicationId().equals(other.getApplicationId()))
      return false;
    if (this.getAttemptId() != other.getAttemptId())
      return false;
    return true;
  }

  @Override
  public int compareTo(ApplicationAttemptId other) {
    int compareAppIds = this.getApplicationId().compareTo(
        other.getApplicationId());
    if (compareAppIds == 0) {
      return this.getAttemptId() - other.getAttemptId();
    } else {
      return compareAppIds;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(appAttemptIdStrPrefix);
    sb.append(this.getApplicationId().getClusterTimestamp()).append("_");
    sb.append(ApplicationId.appIdFormat.get().format(
        this.getApplicationId().getId()));
    sb.append("_").append(attemptIdFormat.get().format(getAttemptId()));
    return sb.toString();
  }

  protected abstract void build();

  @Public
  @Stable
  public static ApplicationAttemptId fromString(String appAttemptIdStr) {
    if (!appAttemptIdStr.startsWith(APP_ATTEMPT_ID_PREFIX)) {
      throw new IllegalArgumentException("Invalid AppAttemptId prefix: "
              + appAttemptIdStr);
    }
    try {
      int pos1 = APP_ATTEMPT_ID_PREFIX.length() - 1;
      int pos2 = appAttemptIdStr.indexOf('_', pos1 + 1);
      if (pos2 < 0) {
        throw new IllegalArgumentException("Invalid AppAttemptId: "
                + appAttemptIdStr);
      }
      long rmId = Long.parseLong(appAttemptIdStr.substring(pos1 + 1, pos2));
      int pos3 = appAttemptIdStr.indexOf('_', pos2 + 1);
      if (pos3 < 0) {
        throw new IllegalArgumentException("Invalid AppAttemptId: "
                + appAttemptIdStr);
      }
      int appId = Integer.parseInt(appAttemptIdStr.substring(pos2 + 1, pos3));
      ApplicationId applicationId = ApplicationId.newInstance(rmId, appId);
      int attemptId = Integer.parseInt(appAttemptIdStr.substring(pos3 + 1));
      ApplicationAttemptId applicationAttemptId =
              ApplicationAttemptId.newInstance(applicationId, attemptId);
      return applicationAttemptId;
    } catch (NumberFormatException n) {
      throw new IllegalArgumentException("Invalid AppAttemptId: "
              + appAttemptIdStr, n);
    }
  }
}
