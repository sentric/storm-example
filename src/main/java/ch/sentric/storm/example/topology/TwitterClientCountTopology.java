/*
 * Copyright 2012 Sentric. All rights reserved.
 */
package ch.sentric.storm.example.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import ch.sentric.storm.example.bolt.ClientExtractorBolt;
import ch.sentric.storm.example.bolt.RedisIncrementBolt;
import ch.sentric.storm.example.spout.TwitterSampleSpout;


public class TwitterClientCountTopology {
	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		final TopologyBuilder builder = new TopologyBuilder();
        final String username = args[0];
        final String pwd = args[1];

		final String redisHost = "127.0.0.1";
		final int redisPort = 6379;
		final int redisDb = 2;

		builder.setSpout("sample", new TwitterSampleSpout(username, pwd));
		builder.setBolt("client", new ClientExtractorBolt()).shuffleGrouping("sample");
		builder.setBolt("redis", new RedisIncrementBolt("clients", redisHost, redisPort, redisDb)).shuffleGrouping("client");
		final Config conf = new Config();

		final LocalCluster cluster = new LocalCluster();

		cluster.submitTopology("test", conf, builder.createTopology());

		Utils.sleep(5 * 60 * 1000);
		cluster.shutdown();
	}

}
