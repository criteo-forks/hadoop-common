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

package org.apache.hadoop.fs.s3a;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.lang.reflect.Field;

import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.http.HttpStatus;

public class TestS3AConfiguration {
  private Configuration conf;
  private S3AFileSystem fs;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3AConfiguration.class);

  private static final String TEST_ENDPOINT = "test.fs.s3a.endpoint";

  @Rule
  public Timeout testTimeout = new Timeout(30 * 60 * 1000);

  /**
   * Test if custom endpoint is picked up.
   * <p/>
   * The test expects TEST_ENDPOINT to be defined in the Configuration
   * describing the endpoint of the bucket to which TEST_FS_S3A_NAME points
   * (f.i. "s3-eu-west-1.amazonaws.com" if the bucket is located in Ireland).
   * Evidently, the bucket has to be hosted in the region denoted by the
   * endpoint for the test to succeed.
   * <p/>
   * More info and the list of endpoint identifiers:
   * http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
   *
   * @throws Exception
   */
  @Test
  public void TestEndpoint() throws Exception {
    conf = new Configuration();
    String endpoint = conf.getTrimmed(TEST_ENDPOINT, "");
    if (endpoint.isEmpty()) {
      LOG.warn("Custom endpoint test skipped as " + TEST_ENDPOINT + "config " +
          "setting was not detected");
    } else {
      conf.set(Constants.ENDPOINT, endpoint);
      fs = S3ATestUtils.createTestFileSystem(conf);
      AmazonS3Client s3 = fs.getAmazonS3Client();
      String endPointRegion = "";
      // Differentiate handling of "s3-" and "s3." based endpoint identifiers
      String[] endpointParts = StringUtils.split(endpoint, '.');
      if (endpointParts.length == 3) {
        endPointRegion = endpointParts[0].substring(3);
      } else if (endpointParts.length == 4) {
        endPointRegion = endpointParts[1];
      } else {
        fail("Unexpected endpoint");
      }
      assertEquals("Endpoint config setting and bucket location differ: ",
          endPointRegion, s3.getBucketLocation(fs.getUri().getHost()));
    }
  }

  @Test
  public void shouldBeAbleToSwitchOnS3PathStyleAccessViaConfigProperty() throws Exception {

    conf = new Configuration();
    conf.set(Constants.PATH_STYLE_ACCESS, Boolean.toString(true));
    assertTrue(conf.getBoolean(Constants.PATH_STYLE_ACCESS, false));

    try {
      fs = S3ATestUtils.createTestFileSystem(conf);
      final Object object = getClientOptionsField(fs.getAmazonS3Client(), "clientOptions");
      assertNotNull(object);
      assertTrue("Unexpected type found for clientOptions!", object instanceof S3ClientOptions);
      assertTrue("Expected to find path style access to be switched on!", ((S3ClientOptions) object).isPathStyleAccess());
      byte[] file = ContractTestUtils.toAsciiByteArray("test file");
      ContractTestUtils.writeAndRead(fs, new Path("/path/style/access/testFile"), file, file.length, conf.getInt(Constants.FS_S3A_BLOCK_SIZE, file.length), false, true);
    } catch (final AmazonS3Exception e) {
      LOG.error("Caught exception: ", e);
      // Catch/pass standard path style access behaviour when live bucket
      // isn't in the same region as the s3 client default. See
      // http://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html
      assertEquals(e.getStatusCode(), HttpStatus.SC_MOVED_PERMANENTLY);
    }
  }

  private Object getClientOptionsField(AmazonS3Client s3client, String field)
      throws NoSuchFieldException, IllegalAccessException {
    final Field clientOptionsProps = s3client.getClass().getDeclaredField(field);
    assertNotNull(clientOptionsProps);
    if (!clientOptionsProps.isAccessible()) {
      clientOptionsProps.setAccessible(true);
    }
    final Object object = clientOptionsProps.get(s3client);
    return object;
  }
}
