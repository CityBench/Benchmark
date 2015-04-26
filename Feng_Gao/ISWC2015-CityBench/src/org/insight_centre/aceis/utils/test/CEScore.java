package org.insight_centre.aceis.utils.test;

public class CEScore {
	private long time, initTime;
	private double initAvgUtility, convergenceAvgUtility, maxUtility;

	public CEScore(long time, long initTime, double initAvgUtility, double convergenceAvgUtility, double maxUtility) {
		super();
		this.time = time;
		this.initTime = initTime;
		this.initAvgUtility = initAvgUtility;
		this.convergenceAvgUtility = convergenceAvgUtility;
		this.maxUtility = maxUtility;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public double getInitAvgUtility() {
		return initAvgUtility;
	}

	public void setInitAvgUtility(double initAvgUtility) {
		this.initAvgUtility = initAvgUtility;
	}

	public double getConvergenceAvgUtility() {
		return convergenceAvgUtility;
	}

	public void setConvergenceAvgUtility(double convergenceAvgUtility) {
		this.convergenceAvgUtility = convergenceAvgUtility;
	}

	public double getMaxUtility() {
		return maxUtility;
	}

	public void setMaxUtility(double maxUtility) {
		this.maxUtility = maxUtility;
	}

	public double getCEScoreUsingAvgUtility() {
		return (this.convergenceAvgUtility - this.initAvgUtility) * 100000 / this.time;
	}

	public double getCEScoreUsingMaxUtility() {
		return (this.maxUtility - this.initAvgUtility) * 100000 / this.time;
	}

	public double getCEScoreGainUsingMaxUtility() {
		return (this.maxUtility - this.initAvgUtility) * 10000 / (this.time - this.initTime);
	}

	public long getInitTime() {
		return initTime;
	}

	public void setInitTime(long initTime) {
		this.initTime = initTime;
	}
}
