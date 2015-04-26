package org.insight_centre.aceis.observations;

import java.util.Date;

import org.insight_centre.aceis.eventmodel.QosVector;

public class QosUpdate implements Comparable<QosUpdate> {
	private String id, correspondingServiceId;
	private QosVector qos;
	private Date sysTimestamp, obTimestamp;

	public QosUpdate(Date obTimestamp, String id, String correspondingServiceId, QosVector qos) {
		super();
		this.sysTimestamp = new Date();
		this.obTimestamp = obTimestamp;
		this.id = id;
		this.correspondingServiceId = correspondingServiceId;
		this.qos = qos;
	}

	@Override
	public int compareTo(QosUpdate other) {
		return this.obTimestamp.compareTo(other.getObTimestamp());
	}

	public String getCorrespondingServiceId() {
		return correspondingServiceId;
	}

	public String getId() {
		return id;
	}

	public Date getObTimestamp() {
		return obTimestamp;
	}

	public QosVector getQos() {
		return qos;
	}

	public Date getSysTimestamp() {
		return sysTimestamp;
	}

	public void setCorrespondingServiceId(String correspondingServiceId) {
		this.correspondingServiceId = correspondingServiceId;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setObTimestamp(Date obTimestamp) {
		this.obTimestamp = obTimestamp;
	}

	public void setQos(QosVector qos) {
		this.qos = qos;
	}

	public void setSysTimestamp(Date sysTimestamp) {
		this.sysTimestamp = sysTimestamp;
	}

	public String toString() {
		return "qos id: " + this.id + ", sensorId: " + this.correspondingServiceId + ", @" + this.obTimestamp
				+ ", qos: " + qos.toString();

	}
}
