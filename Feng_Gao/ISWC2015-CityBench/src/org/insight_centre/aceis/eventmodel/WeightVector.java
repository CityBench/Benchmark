package org.insight_centre.aceis.eventmodel;

/**
 * @author feng
 * 
 *         A data structure to model the users' preferences on the qos attributes
 */
public class WeightVector {
	private Double latencyW, priceW, securityW, accuracyW, reliabilityW, trafficW;

	public WeightVector() {

	}

	public WeightVector(Double latencyW, Double priceW, Double securityW, Double accuracyW, Double reliabilityW,
			Double trafficW) {
		super();
		this.latencyW = latencyW;
		this.priceW = priceW;
		this.securityW = securityW;
		this.accuracyW = accuracyW;
		this.reliabilityW = reliabilityW;
		this.trafficW = trafficW;
	}

	public Double getAccuracyW() {
		return accuracyW;
	}

	public Double getLatencyW() {
		return latencyW;
	}

	public Double getPriceW() {
		return priceW;
	}

	public Double getReliabilityW() {
		return reliabilityW;
	}

	public Double getSecurityW() {
		return securityW;
	}

	public void setAccuracyW(Double accuracyW) {
		this.accuracyW = accuracyW;
	}

	public void setLatencyW(Double latencyW) {
		this.latencyW = latencyW;
	}

	public void setPriceW(Double priceW) {
		this.priceW = priceW;
	}

	public void setReliabilityW(Double reliabilityW) {
		this.reliabilityW = reliabilityW;
	}

	public void setSecurityW(Double securityW) {
		this.securityW = securityW;
	}

	public String toString() {
		return "L:" + this.latencyW + ", P:" + this.priceW + ", S:" + this.securityW + ", A:" + this.accuracyW + ", R:"
				+ this.reliabilityW + ", T:" + this.trafficW;
	}

	public Double getTrafficW() {
		return trafficW;
	}

	public void setTrafficW(Double trafficW) {
		this.trafficW = trafficW;
	}

}
