package org.insight_centre.aceis.observations;

public class AarhusParkingObservation extends SensorObservation {
	String garageCode, streetName;

	double lat, lon;

	int vacancies;

	public AarhusParkingObservation(int vacancies, String garageCode, String streetName, double lat, double lon) {
		super();
		this.vacancies = vacancies;
		this.garageCode = garageCode;
		this.streetName = streetName;
		this.lat = lat;
		this.lon = lon;
	}
	public String getGarageCode() {
		return garageCode;
	}

	public double getLat() {
		return lat;
	}

	public double getLon() {
		return lon;
	}

	public String getStreetName() {
		return streetName;
	}

	public int getVacancies() {
		return vacancies;
	}

	public void setGarageCode(String garageCode) {
		this.garageCode = garageCode;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public void setStreetName(String streetName) {
		this.streetName = streetName;
	}

	public void setVacancies(int vacancies) {
		this.vacancies = vacancies;
	}

}
