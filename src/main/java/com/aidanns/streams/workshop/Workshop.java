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
		
		// Setup the bolts
		
		// Start the job.
		Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxTaskParallelism(3);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("project", conf, builder.createTopology());
		
		try {
			Thread.sleep(1000 * 10); // Run for half a minute.
		} catch (InterruptedException e) {
			Logger.getLogger(Project.class).error("Interrupted while"
					+ " waiting for local cluster to complete processing.");
			e.printStackTrace();
		}
		
		cluster.shutdown();
		System.exit(0);
	}
}
