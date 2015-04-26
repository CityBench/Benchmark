package org.insight_centre.aceis.observations;

import java.util.Date;

public class PollutionObservation extends SensorObservation {
	double pm1, pm2_5, pm10;
	int ozone, particullate_matter, carbon_monoxide, sulfure_dioxide, nitrogen_dioxide;
	int api = 0;

	public int getApi() {
		return api;
	}

	public void setApi(int api) {
		this.api = api;
	}

	public int getOzone() {
		return ozone;
	}

	public PollutionObservation(double pm1, double pm2_5, double pm10, int ozone, int particullate_matter,
			int carbon_monoxide, int sulfure_dioxide, int nitrogen_dioxide, Date timeStamp) {
		super();
		this.pm1 = pm1;
		this.pm2_5 = pm2_5;
		this.pm10 = pm10;
		this.ozone = ozone;
		this.particullate_matter = particullate_matter;
		this.carbon_monoxide = carbon_monoxide;
		this.sulfure_dioxide = sulfure_dioxide;
		this.nitrogen_dioxide = nitrogen_dioxide;
		this.obTimeStamp = timeStamp;

		api = ozone;
		if (this.particullate_matter > this.api)
			api = this.particullate_matter;
		if (this.carbon_monoxide > this.api)
			this.api = this.carbon_monoxide;
		if (this.sulfure_dioxide > this.api)
			this.api = this.sulfure_dioxide;
		if (this.nitrogen_dioxide > this.api)
			this.api = this.nitrogen_dioxide;
	}

	public void setOzone(int ozone) {
		this.ozone = ozone;
	}

	public int getParticullate_matter() {
		return particullate_matter;
	}

	public void setParticullate_matter(int particullate_matter) {
		this.particullate_matter = particullate_matter;
	}

	public int getCarbon_monoxide() {
		return carbon_monoxide;
	}

	public void setCarbon_monoxide(int carbon_monoxide) {
		this.carbon_monoxide = carbon_monoxide;
	}

	public int getSulfure_dioxide() {
		return sulfure_dioxide;
	}

	public void setSulfure_dioxide(int sulfure_dioxide) {
		this.sulfure_dioxide = sulfure_dioxide;
	}

	public int getNitrogen_dioxide() {
		return nitrogen_dioxide;
	}

	public void setNitrogen_dioxide(int nitrogen_dioxide) {
		this.nitrogen_dioxide = nitrogen_dioxide;
	}

	public double getPm1() {
		return pm1;
	}

	public void setPm1(double pm1) {
		this.pm1 = pm1;
	}

	public double getPm2_5() {
		return pm2_5;
	}

	public void setPm2_5(double pm2_5) {
		this.pm2_5 = pm2_5;
	}

	public double getPm10() {
		return pm10;
	}

	public void setPm10(double pm10) {
		this.pm10 = pm10;
	}
}
