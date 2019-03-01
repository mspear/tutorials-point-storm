package jvm.org.michael.storm

import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values

class CallLogCreatorBolt: IRichBolt {
    private lateinit var collector: OutputCollector
    override fun prepare(conf: MutableMap<Any?, Any?>?, context: TopologyContext?, collector: OutputCollector?) {
        this.collector = collector!!
    }

    override fun cleanup() {}

    override fun getComponentConfiguration(): MutableMap<String, Any>? {
        return null
    }

    override fun execute(tuple: Tuple?) {
        val from = tuple!!.getString(0)
        val to = tuple.getString(1)
        val duration = tuple.getInteger(2)
        collector.emit(Values("$from - $to", duration))
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
        declarer!!.declare(Fields("call", "duration"))
    }
}
