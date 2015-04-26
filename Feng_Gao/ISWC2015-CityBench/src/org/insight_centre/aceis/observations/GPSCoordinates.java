package org.insight_centre.aceis.observations;

public class GPSCoordinates {
	private Double lat, lon;

	public Double getLat() {
		return lat;
	}

	public void setLat(Double lat) {
		this.lat = lat;
	}

	public Double getLon() {
		return lon;
	}

	public void setLon(Double lon) {
		this.lon = lon;
	}

	public GPSCoordinates(Double lat, Double lon) {
		super();
		this.lat = lat;
		this.lon = lon;
	}

}
