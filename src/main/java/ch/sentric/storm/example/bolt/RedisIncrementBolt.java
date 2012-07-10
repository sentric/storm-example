/*
 * Copyright 2011 MeMo News AG. All rights reserved.
 */
package ch.sentric.storm.example.bolt;

import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * Increment key in sorted set
 */
public class RedisIncrementBolt extends BaseRichBolt {
	private final String key;
	private final String redisHost;
	private final int redisPort;
	private final int redisDb;
	private Jedis redis;
	private OutputCollector collector;

	/**
	 * @param key
	 * @param redisHost
	 * @param redisPort
	 * @param redisDb
	 */
	public RedisIncrementBolt(final String key, final String redisHost, final int redisPort, final int redisDb) {
		super();
		this.key = key;
		this.redisHost = redisHost;
		this.redisPort = redisPort;
		this.redisDb = redisDb;
	}

	@Override
	public void prepare(final Map stormConf, final TopologyContext context,
			final OutputCollector collector) {
		redis = new Jedis(redisHost, redisPort);
		redis.select(redisDb);
		this.collector = collector;
	}

	@Override
	public void execute(final Tuple input) {
		final String client = input.getString(0);
		redis.zincrby(key, 1.0, client);
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
	}

}
