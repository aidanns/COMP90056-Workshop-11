package com.aidanns.streams.workshop;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class RatingEstimator extends BaseRichBolt {

	private static Logger logger = Logger.getLogger(DebuggingPrinterBolt.class);
	
	private Map<Integer, Integer> _userIdToNumberOfRatingsMap = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> _userIdToTotalRatingMap = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> _venueIdToNumberOfRatingsMap = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> _venueIdToTotalRatingMap = new HashMap<Integer, Integer>();
	
	private int _ratingsProcessed = 0;
	private int _cumulativeDifference = 0;
	
	private Double averageRatingForUser(Integer userId) {
		if (_userIdToNumberOfRatingsMap.containsKey(userId) && _userIdToTotalRatingMap.containsKey(userId)) {
			int totalRating = _userIdToTotalRatingMap.get(userId);
			int numberOfRatings = _userIdToNumberOfRatingsMap.get(userId);
			if (numberOfRatings != 0) {
				return totalRating / (double) numberOfRatings;
			}
		}
		return null;
	}
	
	private Double averageRatingForVenue(Integer venueId) {
		if (_venueIdToNumberOfRatingsMap.containsKey(venueId) && _venueIdToTotalRatingMap.containsKey(venueId)) {
			int totalRating = _venueIdToTotalRatingMap.get(venueId);
			int numberOfRatings = _venueIdToNumberOfRatingsMap.get(venueId);
			if (numberOfRatings != 0) {
				return totalRating / (double) numberOfRatings;
			}
		}
		return null;
	}
	
	private Integer estimateRatingForVenue(Integer userId, Integer venueId) {
		Double averageUserRating = averageRatingForUser(userId);
		Double averageVenueRating = averageRatingForVenue(venueId);
		
		if (averageUserRating != null && averageVenueRating != null) {
			return new Double((averageUserRating + averageVenueRating) / 2.0).intValue();
		} else if (averageUserRating != null) {
			return averageUserRating.intValue();
		} else if (averageVenueRating != null) {
			return averageVenueRating.intValue();
		} else {
			return 3;
		}
	}
	
	private void persistRating(Integer userId, Integer venueId, Integer rating) {
		createRecordForUserIfNeeded(userId);
		createRecordForVenueIfNeeded(venueId);
		addRatingForUser(userId, rating);
		addRatingForVenue(venueId, rating);
	}
	
	private void addRatingForVenue(Integer venueId, Integer rating) {
		_venueIdToNumberOfRatingsMap.put(venueId, _venueIdToNumberOfRatingsMap.get(venueId) + 1);
		_venueIdToTotalRatingMap.put(venueId, _venueIdToTotalRatingMap.get(venueId) + rating);
	}

	private void addRatingForUser(Integer userId, Integer rating) {
		_userIdToNumberOfRatingsMap.put(userId, _userIdToNumberOfRatingsMap.get(userId) + 1);
		_userIdToTotalRatingMap.put(userId, _userIdToTotalRatingMap.get(userId) + rating);
	}

	private void createRecordForVenueIfNeeded(Integer venueId) {
		if (!_venueIdToNumberOfRatingsMap.containsKey(venueId) || !_venueIdToTotalRatingMap.containsKey(venueId)) {
			_venueIdToNumberOfRatingsMap.put(venueId, 0);
			_venueIdToTotalRatingMap.put(venueId, 0);
		}
	}

	private void createRecordForUserIfNeeded(Integer userId) {
		if (!_userIdToNumberOfRatingsMap.containsKey(userId) || !_userIdToTotalRatingMap.containsKey(userId)) {
			_userIdToNumberOfRatingsMap.put(userId, 0);
			_userIdToTotalRatingMap.put(userId, 0);
		}
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {}

	public void execute(Tuple input) {
		Integer userId = (Integer) input.getValueByField("UserId");
		Integer venueId = (Integer) input.getValueByField("VenueId");
		Integer rating = (Integer) input.getValueByField("Rating");
		Integer estimate = estimateRatingForVenue(userId, venueId);
		persistRating(userId, venueId, rating);
		
		_ratingsProcessed++;
		_cumulativeDifference += Math.abs(estimate - rating);
		
		logger.info(String.format("User: %10d Venue: %10d Estimate: %3d Actual: %3d Average difference: %3.2f", userId, venueId, estimate, rating, this.getAverageRatingDifference()));
	}

	private Double getAverageRatingDifference() {
		return _ratingsProcessed == 0 ? 0.0 : (double) _cumulativeDifference / (double) _ratingsProcessed;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
