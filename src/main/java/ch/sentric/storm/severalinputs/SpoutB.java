/**
 * 
 */
package ch.sentric.storm.severalinputs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 *
 */
public class SpoutB extends BaseRichSpout {
	private static final long serialVersionUID = -2920227546713932302L;

	SpoutOutputCollector _collector;
	int counter = 0;
	List<Values> emittedTuples = new ArrayList<Values>(0);

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		 _collector = collector;
	}

	@Override
	public void nextTuple() {
		counter++;
		Values val = new Values("B-" + counter);
		_collector.emit(val);
		emittedTuples.add(val);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public List<Values> getEmittedTuples() {
		return emittedTuples;
	}

}
