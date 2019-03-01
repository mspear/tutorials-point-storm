package jvm.org.michael.storm

import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichSpout
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Values
import java.util.*

class FakeCallLogReaderSpout : IRichSpout{

    private lateinit var collector : SpoutOutputCollector
    private lateinit var context : TopologyContext

    private var completed = false
    private val randomGenerator = Random()
    private var idx = 0
    override fun deactivate() {}

    override fun open(conf: MutableMap<Any?, Any?>?, context: TopologyContext?, collector: SpoutOutputCollector) {
        this.context = context!!
        this.collector = collector
    }

    override fun nextTuple() {
        if (idx <= 1000) {
            val mobileNumbers = listOf("1234123401", "1234123402", "1234123403", "1234123404")

            var localIdx = 0
            while (localIdx++ <= 100 && idx++ <= 1000) {
                val fromMobileNumber = mobileNumbers[randomGenerator.nextInt(4)]
                var toMobileNumber : String
                do {
                    toMobileNumber = mobileNumbers[randomGenerator.nextInt(4)]
                } while (toMobileNumber == fromMobileNumber)
                val duration = randomGenerator.nextInt(60)
                collector.emit(Values(fromMobileNumber, toMobileNumber, duration))
            }
        }
    }

    override fun activate() {}

    override fun fail(p0: Any?) {}

    override fun getComponentConfiguration(): MutableMap<String, Any>? {
        return null
    }

    override fun ack(p0: Any?) {}

    override fun close() {}

    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
        declarer!!.declare(Fields("from", "to", "duration"))
    }

}
