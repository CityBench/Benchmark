package org.insight_centre.aceis.eventmodel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author feng
 * 
 *         A data structure for qos vector
 */
public class QosVector implements Cloneable {

	private double accuracy, reliability, traffic;
	private int latency, price, security;

	// private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public QosVector(int latency, int price, int security, Double accuracy, Double reliability, Double traffic) {
		super();
		this.latency = latency;
		this.price = price;
		this.security = security;
		this.accuracy = accuracy;
		this.reliability = reliability;
		this.traffic = traffic;
	}

	public static QosVector getRandomQos() {
		int l = (int) (Math.random() * 300);
		int p = (int) (Math.random() * 300);
		int s = (int) (Math.random() * 5) + 1;
		Double acc = (Math.random() * 1.0) + 0.0;
		Double rel = (Math.random() * 1.0) + 0.0;

		QosVector qos = new QosVector(l, p, s, acc, rel, 0.0);
		return qos;

	}

	public static QosVector getGoodRandomQos() {
		int l = (int) (Math.random() * 300);
		int p = (int) (Math.random() * 300);
		int s = (int) (Math.random() * 5) + 1;
		Double acc = (Math.random() * 0.2) + 0.8;
		Double rel = (Math.random() * 0.2) + 0.8;

		QosVector qos = new QosVector(l, p, s, acc, rel, 0.0);
		return qos;

	}

	public QosVector() {

	}

	public QosVector clone() {
		QosVector qv = new QosVector(latency, price, security, accuracy, reliability, traffic);
		return qv;

	}

	public Double getAccuracy() {
		return accuracy;
	}

	public int getLatency() {
		return latency;
	}

	public int getPrice() {
		return price;
	}

	public Double getReliability() {
		return reliability;
	}

	public int getSecurity() {
		return security;
	}

	public void setAccuracy(Double accuracy) {
		this.accuracy = accuracy;
	}

	public void setLatency(int latency) {
		this.latency = latency;
	}

	public void setPrice(int price) {
		this.price = price;
	}

	public void setReliability(Double reliability) {
		this.reliability = reliability;
	}

	public void setSecurity(int security) {
		this.security = security;
	}

	public boolean satisfyConstraint(QosVector constraint) {
		if (this.getAccuracy() >= constraint.getAccuracy() && this.getReliability() >= constraint.getReliability()
				&& this.getSecurity() >= constraint.getSecurity() && this.getLatency() <= constraint.getLatency()
				&& this.getPrice() <= constraint.getPrice())
			return true;
		return false;
	}

	public String toString() {
		return "L:" + this.latency + ", P:" + this.price + ", S:" + this.security + ", A:" + this.accuracy + ", R:"
				+ this.reliability + ", T:" + this.traffic;

	}

	public boolean equals(QosVector other) {
		// logger.info("comparing: " + this.toString() + " with: " + other.toString());
		// boolean result = true;
		if (this.accuracy != other.getAccuracy())
			return false;
		if (this.reliability != other.getReliability())
			return false;
		if (this.traffic != other.getTraffic())
			return false;
		if (this.latency != other.getLatency())
			return false;
		if (this.price != other.getPrice())
			return false;
		if (this.security != other.getPrice())
			return false;

		return true;

	}

	public Double getTraffic() {
		return traffic;
	}

	public void setTraffic(Double traffic) {
		this.traffic = traffic;
	}
}
