package org.insight_centre.aceis.observations;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by sefki on 11/06/2014.
 */
public class AarhusTrafficObservation extends SensorObservation {
	double averageSpeed;
	double avgMeasuredTime;
	String city_1;
	String city_2;
	double congestionLevel;
	String country1;
	String country2;
	double estimatedTime;
	double latitude1;
	double latitude2;
	double longtitude1;
	double longtitude2;
	double Report_ID;
	String street1;
	String street2;
	String timestamp;
	double vehicle_count;

	public AarhusTrafficObservation() {

	}

	public AarhusTrafficObservation(double report_ID, double averageSpeed, double vehicle_count,
			double avgMeasuredTime, double estimatedTime, double congestionLevel, String street1, String city_1,
			double latitude1, double longtitude1, String street2, String city_2, double latitude2, double longtitude2,
			String country1, String country2, String timestamp) {
		super();
		this.Report_ID = report_ID;
		this.averageSpeed = averageSpeed;
		this.vehicle_count = vehicle_count;
		this.avgMeasuredTime = avgMeasuredTime;
		this.estimatedTime = estimatedTime;
		this.congestionLevel = congestionLevel;
		this.street1 = street1;
		this.city_1 = city_1;
		this.latitude1 = latitude1;
		this.longtitude1 = longtitude1;
		this.street2 = street2;
		this.city_2 = city_2;
		this.latitude2 = latitude2;
		this.longtitude2 = longtitude2;
		this.country1 = country1;
		this.country2 = country2;
		this.timestamp = timestamp;
		this.obTimeStamp = this.toTimeStamp(this.timestamp);
		// super.se
	}

	public double getAverageSpeed() {
		return averageSpeed;
	}

	// public StreamData(double report_ID, double averageSpeed, double vehicle_count, double avgMeasuredTime,
	// double estimatedTime, double congestionLevel) {
	// Report_ID = report_ID;
	// this.averageSpeed = averageSpeed;
	// this.vehicle_count = vehicle_count;
	// this.avgMeasuredTime = avgMeasuredTime;
	// this.estimatedTime = estimatedTime;
	// this.congestionLevel = congestionLevel;
	// }
	//
	// public StreamData(double report_ID, double averageSpeed, double vehicle_count, double avgMeasuredTime) {
	// Report_ID = report_ID;
	// this.averageSpeed = averageSpeed;
	// this.vehicle_count = vehicle_count;
	// this.avgMeasuredTime = avgMeasuredTime;
	// }

	public double getAvgMeasuredTime() {
		return avgMeasuredTime;
	}

	public String getCity_1() {
		return city_1;
	}

	public String getCity_2() {
		return city_2;
	}

	public double getCongestionLevel() {
		return congestionLevel;
	}

	public String getCountry1() {
		return country1;
	}

	public String getCountry2() {
		return country2;
	}

	public double getEstimatedTime() {
		return estimatedTime;
	}

	public double getLatitude1() {
		return latitude1;
	}

	public double getLatitude2() {
		return latitude2;
	}

	public double getLongtitude1() {
		return longtitude1;
	}

	public double getLongtitude2() {
		return longtitude2;
	}

	public double getReport_ID() {
		return Report_ID;
	}

	public String getStreet1() {
		return street1;
	}

	public String getStreet2() {
		return street2;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public double getVehicle_count() {
		return vehicle_count;
	}

	public void setAverageSpeed(double averageSpeed) {
		this.averageSpeed = averageSpeed;
	}

	public void setAvgMeasuredTime(double avgMeasuredTime) {
		this.avgMeasuredTime = avgMeasuredTime;
	}

	public void setCity_1(String city_1) {
		this.city_1 = city_1;
	}

	public void setCity_2(String city_2) {
		this.city_2 = city_2;
	}

	public void setCongestionLevel(double congestionLevel) {
		this.congestionLevel = congestionLevel;
	}

	public void setCountry1(String country1) {
		this.country1 = country1;
	}

	public void setCountry2(String country2) {
		this.country2 = country2;
	}

	public void setEstimatedTime(double estimatedTime) {
		this.estimatedTime = estimatedTime;
	}

	public void setLatitude1(double latitude1) {
		this.latitude1 = latitude1;
	}

	public void setLatitude2(double latitude2) {
		this.latitude2 = latitude2;
	}

	public void setLongtitude1(double longtitude1) {
		this.longtitude1 = longtitude1;
	}

	public void setLongtitude2(double longtitude2) {
		this.longtitude2 = longtitude2;
	}

	public void setReport_ID(double report_ID) {
		Report_ID = report_ID;
	}

	public void setStreet1(String street1) {
		this.street1 = street1;
	}

	public void setStreet2(String street2) {
		this.street2 = street2;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public void setVehicle_count(double vehicle_count) {
		this.vehicle_count = vehicle_count;
	}

	@Override
	public String toString() {
		return "StreamData{" + "Report_ID=" + Report_ID + ", averageSpeed=" + averageSpeed + ", vehicle_count="
				+ vehicle_count + ", avgMeasuredTime=" + avgMeasuredTime + ", estimatedTime=" + estimatedTime
				+ ", congestionLevel=" + congestionLevel + ", timestamp=" + timestamp + ", street1='" + street1 + '\''
				+ ", city_1='" + city_1 + '\'' + ", latitude1=" + latitude1 + ", longtitude1=" + longtitude1
				+ ", street2='" + street2 + '\'' + ", city_2='" + city_2 + '\'' + ", latitude2=" + latitude2
				+ ", longtitude2=" + longtitude2 + ", country1='" + country1 + '\'' + ", country2='" + country2 + '\''
				+ '}';
	}

	@SuppressWarnings("finally")
	private Date toTimeStamp(String timestamp) {
		Date date = null;
		try {
			date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(timestamp);
		} catch (ParseException e) {
			e.printStackTrace();
		} finally {
			return date;
		}
	}

}
