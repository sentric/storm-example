/*
 * Copyright 2011 MeMo News AG. All rights reserved.
 */
package ch.sentric.storm.example.bolt;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ClientExtractorBolt extends BaseRichBolt {
	private OutputCollector collector;
	Pattern clientPattern = Pattern.compile("<a[^>]+>([^<]+)</a>");

	@Override
	public void prepare(final Map stormConf, final TopologyContext context,
			final OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(final Tuple input) {
		final Status status = (Status) input.getValue(0);
		final String client = status.getSource();
		if (client.equals("web")) {
			collector.emit(input, new Values(client));
		} else {
			final Matcher matcher = clientPattern.matcher(client);
			if (matcher.matches()) {
				collector.emit(input, new Values(matcher.group(1)));
			} else {
				collector.emit(input, new Values("unknown"));
			}
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("client"));
	}

}
