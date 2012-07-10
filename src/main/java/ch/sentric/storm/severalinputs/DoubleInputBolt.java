package ch.sentric.storm.severalinputs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class DoubleInputBolt extends BaseRichBolt {

	private static final long serialVersionUID = 4424843756581557740L;
	private OutputCollector collector;
	List<Tuple> receivedTuples = new ArrayList<Tuple>(0);

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		System.out.println("Incoming tuple: " + input.getString(0));
		receivedTuples.add(input);
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	public List<Tuple> getReceivedTuples() {
		return receivedTuples;
	}

}
