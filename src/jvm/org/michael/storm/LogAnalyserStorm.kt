package jvm.org.michael.storm

import org.apache.storm.Config
import org.apache.storm.LocalCluster
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.tuple.Fields

fun main() {
    val config = Config()
    config.setDebug(true)

    val builder = TopologyBuilder()
    builder.setSpout("call-log-reader-spout", FakeCallLogReaderSpout())

    builder.setBolt("call-log-creator-bolt", CallLogCreatorBolt())
        .shuffleGrouping("call-log-reader-spout")
    builder.setBolt("call-log-counter-bolt", CallLogCounterBolt())
        .fieldsGrouping("call-log-creator-bolt", Fields("call"))

    val cluster = LocalCluster()
    cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology())
    Thread.sleep(10000)

    // Stop the topology
    cluster.shutdown()
}
