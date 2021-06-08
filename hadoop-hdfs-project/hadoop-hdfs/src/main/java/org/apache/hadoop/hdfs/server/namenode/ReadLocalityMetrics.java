package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

@Metrics(name="ReadLocality", about="ReadLocality metrics from namenode", context="dfs")
public class ReadLocalityMetrics {
    final MetricsRegistry registry = new MetricsRegistry("namenode");

    @Metric("Number of placements on the same host as the writer")
    MutableCounterLong hostLocalReads;
    @Metric("Number of placements on the same rack as the writer")
    MutableCounterLong rackLocalReads;
    @Metric("Number of placements on a different rack as the writer")
    MutableCounterLong offSwitchReads;

    ReadLocalityMetrics(String processName, String sessionId) {
        registry.tag(ProcessName, processName).tag(SessionId, sessionId);
    }

    public static ReadLocalityMetrics create(Configuration conf, NamenodeRole r) {
        String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
        String processName = r.toString();
        MetricsSystem ms = DefaultMetricsSystem.instance();

        return ms.register(new ReadLocalityMetrics(processName, sessionId));
    }

    public void shutdown() {
        DefaultMetricsSystem.shutdown();
    }

    public void incrHostLocalReads() {
        hostLocalReads.incr();
    }

    public void incrRackLocalReads() {
        rackLocalReads.incr();
    }

    public void incrOffSwitchReads() {
        offSwitchReads.incr();
    }

}
