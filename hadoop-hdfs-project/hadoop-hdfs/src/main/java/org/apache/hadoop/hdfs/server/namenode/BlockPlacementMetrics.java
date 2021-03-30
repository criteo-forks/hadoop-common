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

@Metrics(name="BlockPlacement", about="BlockPlacement metrics from namenode", context="dfs")
public class BlockPlacementMetrics {
    final MetricsRegistry registry = new MetricsRegistry("namenode");

    @Metric("Number of placement on the same host as the writer")
    MutableCounterLong hostLocalPlacements;
    @Metric("Number of placement on the same rack as the writer")
    MutableCounterLong rackLocalPlacements;
    @Metric("Number of placement on a different rack as the writer")
    MutableCounterLong offSwitchPlacements;
    @Metric("Number of WebHDFS block placement")
    MutableCounterLong webHDFSPlacements;
    @Metric("Metric not computed due to absent clientnode")
    MutableCounterLong absentClientNode;

    BlockPlacementMetrics(String processName, String sessionId) {
        registry.tag(ProcessName, processName).tag(SessionId, sessionId);
    }

    public static BlockPlacementMetrics create(Configuration conf, NamenodeRole r) {
        String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
        String processName = r.toString();
        MetricsSystem ms = DefaultMetricsSystem.instance();

        return ms.register(new BlockPlacementMetrics(processName, sessionId));
    }

    public void shutdown() {
        DefaultMetricsSystem.shutdown();
    }

    public void incrHostLocalPlacements() {
        hostLocalPlacements.incr();
    }

    public void incrRackLocalPlacements() {
        rackLocalPlacements.incr();
    }

    public void incrOffSwitchPlacements() {
        offSwitchPlacements.incr();
    }

    public void incrWebHDFSPlacements() { webHDFSPlacements.incr(); }

    public void incrAbsentClientNode() { absentClientNode.incr(); }

}
