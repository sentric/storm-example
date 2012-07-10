package ch.sentric.storm.severalinputs;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class DoubleInputTopology {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final TopologyBuilder builder = new TopologyBuilder();

		SpoutA aSpout = new SpoutA();
		SpoutB bSpout = new SpoutB();
		builder.setSpout("aSpout", aSpout);
		builder.setSpout("bSpout", bSpout);
		DoubleInputBolt dInputBolt = new DoubleInputBolt();
		builder.setBolt("doubleInBolt", dInputBolt, 5).shuffleGrouping("aSpout").shuffleGrouping("bSpout");
		final Config conf = new Config();

		final LocalCluster cluster = new LocalCluster();

		cluster.submitTopology("doubleInTest", conf, builder.createTopology());

		Utils.sleep(5000);
		cluster.shutdown();
		
		System.out.println("A-Tuples emitted: " + aSpout.getEmittedTuples().size());
		System.out.println("B-Tuples emitted: " + bSpout.getEmittedTuples().size());
		System.out.println("Bolt received: " +dInputBolt.getReceivedTuples().size());

	}

}
