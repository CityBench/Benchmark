package org.insight_centre.aceis.observations;

import java.util.Date;

public class WeatherObservation extends SensorObservation {
	int humidity;
	double windSpeed, temperature;

	public WeatherObservation(double temperature, int humidity, double windSpeed, Date timeStamp) {
		super();
		this.temperature = temperature;
		this.humidity = humidity;
		this.windSpeed = windSpeed;
		this.obTimeStamp = timeStamp;
	}

	public double getTemperature() {
		return temperature;
	}

	public void setTemperature(double temperature) {
		this.temperature = temperature;
	}

	public double getWindSpeed() {
		return windSpeed;
	}

	public void setWindSpeed(double windSpeed) {
		this.windSpeed = windSpeed;
	}

	public int getHumidity() {
		return humidity;
	}

	public void setHumidity(int humidity) {
		this.humidity = humidity;
	}
}
