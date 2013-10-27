package com.aidanns.streams.workshop;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * Logic that starts the program.
 * @author Aidan Nagorcka-Smith (aidann@student.unimelb.edu.au)
 */
public class Workshop {
	
	/**
	 * Start the application.
	 * @param args The arguments to the app.
	 */
	public static void main(String args[]) {
		TopologyBuilder builder = new TopologyBuilder();
		
		// Setup the spouts.
		builder.setSpout("RatingSpout", new RatingSpout(), 1);
		
		// Setup the bolts
		builder.setBolt("RatingBolt", new RatingEstimator(), 1).
				shuffleGrouping("RatingSpout", "Ratings");
		
		// Start the job.
		Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxTaskParallelism(3);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("project", conf, builder.createTopology());
		
		try {
			Thread.sleep(1000 * 300); // Run for 5 minutes.
		} catch (InterruptedException e) {
			Logger.getLogger(Workshop.class).error("Interrupted while"
					+ " waiting for local cluster to complete processing.");
			e.printStackTrace();
		}
		
		cluster.shutdown();
		System.exit(0);
	}
}
