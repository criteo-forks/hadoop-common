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

package org.apache.hadoop.yarn.security.client;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos;

import java.io.*;

@Public
@Evolving
public class ClientToAMTokenIdentifier extends TokenIdentifier {

    public static final Text KIND_NAME = new Text("YARN_CLIENT_TOKEN");

    private ClientToAMTokenIdentifier clientToAMTokenIdentifier;

    // TODO: Add more information in the tokenID such that it is not
    // transferrable, more secure etc.

    public ClientToAMTokenIdentifier() {
    }

    public ClientToAMTokenIdentifier(ApplicationAttemptId id, String client) {
        this();
        clientToAMTokenIdentifier = new ByteArray(id, client);
    }

    public ApplicationAttemptId getApplicationAttemptID() {
        return this.clientToAMTokenIdentifier.getApplicationAttemptID();
    }

    public String getClientName() {
        return this.clientToAMTokenIdentifier.getClientName();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        clientToAMTokenIdentifier.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte[] data = IOUtils.readFullyToByteArray(in);
        try {
            clientToAMTokenIdentifier = new Proto();
            clientToAMTokenIdentifier.readFields(new DataInputStream(new ByteArrayInputStream(data)));
        } catch (InvalidProtocolBufferException e) {
            clientToAMTokenIdentifier = new ByteArray();
            clientToAMTokenIdentifier.readFields(new DataInputStream(new ByteArrayInputStream(data)));
        }
    }

    @Override
    public Text getKind() {
        return KIND_NAME;
    }

    @Override
    public UserGroupInformation getUser() {
        return clientToAMTokenIdentifier.getUser();
    }

    @InterfaceAudience.Private
    public static class Renewer extends Token.TrivialRenewer {
        @Override
        protected Text getKind() {
            return KIND_NAME;
        }
    }

    public class Proto extends ClientToAMTokenIdentifier {
        YarnSecurityTokenProtos.ClientToAMTokenIdentifierProto proto;

        public Proto() {
        }

        public Proto(ApplicationAttemptId id, String client) {
            YarnSecurityTokenProtos.ClientToAMTokenIdentifierProto.Builder builder =
                YarnSecurityTokenProtos.ClientToAMTokenIdentifierProto.newBuilder();
            if (id != null) {
                builder.setAppAttemptId(((ApplicationAttemptIdPBImpl) id).getProto());
            }
            if (client != null) {
                builder.setClientName(client);
            }
            proto = builder.build();
        }

        public ApplicationAttemptId getApplicationAttemptID() {
            if (!proto.hasAppAttemptId()) {
                return null;
            }
            return new ApplicationAttemptIdPBImpl(proto.getAppAttemptId());
        }

        public String getClientName() {
            return proto.getClientName();
        }

        public YarnSecurityTokenProtos.ClientToAMTokenIdentifierProto getProto() {
            return proto;
        }

        @Override
        public Text getKind() {
            return KIND_NAME;
        }

        @Override
        public UserGroupInformation getUser() {
            String clientName = getClientName();
            if (clientName == null) {
                return null;
            }
            return UserGroupInformation.createRemoteUser(clientName);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.write(proto.toByteArray());
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            proto = YarnSecurityTokenProtos.ClientToAMTokenIdentifierProto.parseFrom((DataInputStream)in);
        }
    }

    public class ByteArray extends ClientToAMTokenIdentifier {
        private ApplicationAttemptId applicationAttemptId;
        private Text clientName = new Text();

        public ByteArray() {
        }

        public ByteArray(ApplicationAttemptId id, String client) {
            this.applicationAttemptId = id;
            this.clientName = new Text(client);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(this.applicationAttemptId.getApplicationId()
                .getClusterTimestamp());
            out.writeInt(this.applicationAttemptId.getApplicationId().getId());
            out.writeInt(this.applicationAttemptId.getAttemptId());
            this.clientName.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.applicationAttemptId =
                ApplicationAttemptId.newInstance(
                    ApplicationId.newInstance(in.readLong(), in.readInt()), in.readInt());
            this.clientName.readFields(in);
        }

        public ApplicationAttemptId getApplicationAttemptID() {
            return this.applicationAttemptId;
        }

        public String getClientName() {
            return this.clientName.toString();
        }

        @Override
        public Text getKind() {
            return KIND_NAME;
        }

        @Override
        public UserGroupInformation getUser() {
            if (this.clientName == null) {
                return null;
            }
            return UserGroupInformation.createRemoteUser(this.clientName.toString());
        }

    }
}
