package org.apache.hadoop.yarn.security.client;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.RMDelegationTokenIdentifierDataProto;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

public class RMDelegationTokenIdentifierData {

    private final RMDelegationTokenIdentifierDataProto.Builder builder =
            RMDelegationTokenIdentifierDataProto.newBuilder();

    private final RMDelegationTokenIdentifier identifier;
    private final long renewDate;

    public RMDelegationTokenIdentifierData(DataInputStream in) throws IOException {
        //make a copy of the input stream so that it can be read many times
        byte[] data = IOUtils.readFullyToByteArray(in);
        RMDelegationTokenIdentifier tmpIdentifier;
        long tmpRenewDate;
        try {
            //HDP2 way
            DataInput di = new DataInputStream(new ByteArrayInputStream(data));
            tmpIdentifier = new RMDelegationTokenIdentifier();
            tmpIdentifier.readFieldsOldFormat(di);
            tmpRenewDate = di.readLong();
        } catch (IOException e) {
            //HDP3 using proto
            builder.mergeFrom(data);
            YARNDelegationTokenIdentifierProto tokenIdentifier = builder.getTokenIdentifier();
            tmpIdentifier = new RMDelegationTokenIdentifier();
            tmpIdentifier.setOwner(new Text(tokenIdentifier.getOwner()));
            tmpIdentifier.setRenewer(new Text(tokenIdentifier.getRenewer()));
            tmpIdentifier.setRealUser(new Text(tokenIdentifier.getRealUser()));
            tmpIdentifier.setIssueDate(tokenIdentifier.getIssueDate());
            tmpIdentifier.setMaxDate(tokenIdentifier.getMaxDate());
            tmpIdentifier.setSequenceNumber(tokenIdentifier.getSequenceNumber());
            tmpIdentifier.setMasterKeyId(tokenIdentifier.getMasterKeyId());
            tmpRenewDate = builder.getRenewDate();
        }

        this.identifier = tmpIdentifier;
        this.renewDate = tmpRenewDate;
    }

    public RMDelegationTokenIdentifier getRMDelegationTokenIdentifier() {
        return identifier;
    }

    public long getRenewDate() {
        return renewDate;
    }
}
