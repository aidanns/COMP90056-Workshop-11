package com.aidanns.streams.workshop;

import java.net.URISyntaxException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.Callback;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.Listener;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Spout that reads data from a MQTT topic that Carlos is running.
 * @author Aidan Nagorcka-Smith (aidanns@gmail.com)
 */
@SuppressWarnings("serial")
public class RatingSpout extends BaseRichSpout {
	
	private static String SERVER_IP_ADDRESS = "119.81.5.2";
	private static Integer SERVER_PORT = 1884;
	private static String TOPIC_NAME = "316748";
	
	private static Logger logger = Logger.getLogger(RatingSpout.class);

	private SpoutOutputCollector _collector;
	
	private BlockingQueue<Rating> _ratings;
	
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		_ratings = new LinkedBlockingQueue<Rating>();
		
		// Tell the server to start sending us Ratings.
		
		try {
			MQTT mqttService = new MQTT();
			mqttService.setHost(SERVER_IP_ADDRESS, SERVER_PORT);
			BlockingConnection connection = mqttService.blockingConnection();
			connection.connect();
			connection.publish("comp90056", "START,316748".getBytes(), QoS.AT_LEAST_ONCE, true);
			connection.disconnect();
		} catch (URISyntaxException e) {
			// Can't get here - the URI is hard-coded and well formed.
			logger.error("Threw an error in Subscriber that we didn't think was possible.");
			System.exit(1);
		} catch (Exception e) {
			logger.error("Failed to send message to the server.");
			System.exit(1);
		}
		
		// Subscribe to the Ratings.
		
		MQTT mqttService = new MQTT();
		try {
			mqttService.setHost(SERVER_IP_ADDRESS, SERVER_PORT);
		} catch (URISyntaxException e) {
			logger.error("Got an error that we didn't think was possible.");
			System.exit(1);
		}
		
		Topic myTopic = new Topic(TOPIC_NAME, QoS.AT_LEAST_ONCE);
		final Topic[] topicsToSubscribeTo = { myTopic };
		
		final CallbackConnection connection = mqttService.callbackConnection();
		
		// Set up the handler for receiving new messages.
		connection.listener(new Listener() {
			public void onConnected() {}

			public void onDisconnected() {}

			public void onPublish(UTF8Buffer topic, Buffer body, Runnable ack) {
				if (topic.toString().equals(TOPIC_NAME)) {
					StringTokenizer tokenizer = new StringTokenizer(new String(body.buffer().toByteArray()), ",");
					if (tokenizer.countTokens() != 3) {
						logger.error("Incorrect number of tokens in rating message.");
						System.exit(1);
					}
					Integer userId = Integer.parseInt(tokenizer.nextToken());
					Integer venueId = Integer.parseInt(tokenizer.nextToken());
					Integer rating = Integer.parseInt(tokenizer.nextToken());
					_ratings.add(new Rating(userId, venueId, rating));
				}
				ack.run();
			}

			public void onFailure(Throwable value) {
				logger.error("Connection failed: " + value.toString());
				System.exit(1);
			}
		});
		
		// Connect to the server and subscribe to the topic.
		connection.connect(new Callback<Void>() {
			public void onFailure(Throwable t) {
				logger.error("Rating spout failed to connect to server.");
				System.exit(1);
			}

			public void onSuccess(Void arg0) {
				logger.info("Connection established");
				connection.subscribe(topicsToSubscribeTo, new Callback<byte[]>() {
					public void onFailure(Throwable arg0) {
						logger.error("Failed to subscribe to topic.");
						System.exit(1);
					}

					public void onSuccess(byte[] arg0) {
						logger.info("Subscribed to the topic.");
					}
				});
			}
		});
	}

	public void nextTuple() {
		Rating status = _ratings.poll();
		if (status == null) {
			Utils.sleep(50);
		} else {
			_collector.emit("Ratings", new Values(status.userId(), status.venueId(), status.rating()));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("Ratings", new Fields("UserId", "VenueId", "Rating"));
	}

}
