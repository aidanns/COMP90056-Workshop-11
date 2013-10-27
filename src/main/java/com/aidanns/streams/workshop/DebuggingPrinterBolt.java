package com.aidanns.streams.workshop;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * Bolt that prints what it receives.
 * @author Aidan Nagorcka-Smith (aidanns@gmail.com)
 */
@SuppressWarnings("serial")
public class DebuggingPrinterBolt extends BaseRichBolt {

	public static Logger logger = Logger.getLogger(DebuggingPrinterBolt.class);
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {}

	public void execute(Tuple input) {
		if (input.getSourceStreamId() == "Ratings") {
			logger.info("Got a rating: {UserID: " + 
					input.getValueByField("UserId") + ", VenueId: " + 
					input.getValueByField("VenueId") + ", Rating: " + 
					input.getValueByField("Rating") + "}");
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
