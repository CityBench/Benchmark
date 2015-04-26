package org.insight_centre.aceis.io;

import java.util.Date;
import java.util.List;

public class QosSubscription {
	private List<String> sensorId;
	private Double freq;
	private boolean unsubscribe = false;
	private Date start, end;

	public List<String> getSensorId() {
		return sensorId;
	}

	public void setSensorId(List<String> sensorId) {
		this.sensorId = sensorId;
	}

	public Double getFreq() {
		return freq;
	}

	public void setFreq(Double freq) {
		this.freq = freq;
	}

	public QosSubscription(List<String> sensorId, Double freq, Date start, Date end) {
		super();
		this.sensorId = sensorId;
		this.freq = freq;
		this.start = start;
		this.end = end;
	}

	public boolean isUnsubscribe() {
		return unsubscribe;
	}

	public void setUnsubscribe(boolean unsubscribe) {
		this.unsubscribe = unsubscribe;
	}

	public Date getStart() {
		return start;
	}

	public void setStart(Date start) {
		this.start = start;
	}

	public Date getEnd() {
		return end;
	}

	public void setEnd(Date end) {
		this.end = end;
	}
}
