package com.aidanns.streams.workshop;

/**
 * Representation of a FourSquare rating.
 * @author Aidan Nagorcka-Smith (aidanns@gmail.com)
 */
public class Rating {
	
	private Integer _userId;
	
	private Integer _venueId;
	
	private Integer _rating;

	public Rating(Integer userId, Integer venueId, Integer rating) {
		super();
		this._userId = userId;
		this._venueId = venueId;
		this._rating = rating;
	}

	public Integer userId() {
		return _userId;
	}

	public Integer venueId() {
		return _venueId;
	}

	public Integer rating() {
		return _rating;
	}
	
}
