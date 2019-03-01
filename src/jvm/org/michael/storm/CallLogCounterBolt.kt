package jvm.org.michael.storm

import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple

class CallLogCounterBolt : IRichBolt {
    val counterMap = HashMap<String, Int>()
    private lateinit var collector: OutputCollector
    override fun prepare(p0: MutableMap<Any?, Any?>?, p1: TopologyContext?, collector: OutputCollector) {
        this.collector = collector
    }

    override fun cleanup() {
        for (entry in counterMap.entries) {
            println("${entry.key} : ${entry.value}")
        }
    }

    override fun getComponentConfiguration(): MutableMap<String, Any>? {
        return null
    }

    override fun execute(tuple: Tuple?) {
        val call = tuple!!.getString(0)
        val duration = tuple.getInteger(1)
        counterMap[call] = counterMap.getOrDefault(call, 0) + 1
        collector.ack(tuple)

    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
        declarer!!.declare(Fields("call"))
    }
}
