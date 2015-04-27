package org.insight_centre.aceis.eventmodel;

import java.util.List;

/**
 * @author feng
 * 
 *         An event declaration can be seen as a service description that describes the event service
 * 
 */
public class EventDeclaration implements Cloneable {
	private EventPattern ep;
	private String eventType;
	private String foi;
	private Double frequency;
	private String nodeId;
	private QosVector internalQos, externalQos;

	public void setExternalQos(QosVector externalQos) {
		this.externalQos = externalQos;
	}

	private EventPattern composedFor;

	public EventPattern getComposedFor() {
		return composedFor;
	}

	public void setComposedFor(EventPattern composedFor) {
		this.composedFor = composedFor;
	}

	// private List<Selection> eventPayloads;
	private List<String> payloads;

	private String serviceId;
	private String src;
	private String distance;

	public EventDeclaration(String iD, String src, String eventType, EventPattern ep, List<String> payloads,
			Double frequency) {
		super();
		nodeId = iD;
		this.src = src;
		this.eventType = eventType;
		this.ep = ep;
		this.payloads = payloads;
		this.frequency = frequency;
	}

	public EventDeclaration(String iD, String src, String eventType, EventPattern ep, List<String> payloads,
			Double frequency, QosVector internalQos) {
		super();
		nodeId = iD;
		this.src = src;
		this.eventType = eventType;
		this.ep = ep;
		this.payloads = payloads;
		this.frequency = frequency;
		this.internalQos = internalQos;
	}

	public EventDeclaration clone() throws CloneNotSupportedException {
		return (EventDeclaration) super.clone();
	}

	public EventPattern getEp() {
		return ep;
	}

	public String getEventType() {
		return eventType;
	}

	public QosVector getExternalQos() throws CloneNotSupportedException, NodeRemovalException {
		if (this.ep == null)
			return internalQos;
		else {
			if (this.externalQos != null)
				return this.externalQos;
			QosVector externalQos = new QosVector(0, 0, 0, 0.0, 0.0, 0.0);
			QosVector compositionQos = null;
			externalQos.setAccuracy(internalQos.getAccuracy() * compositionQos.getAccuracy());
			externalQos.setLatency(internalQos.getLatency() + compositionQos.getLatency());
			externalQos.setPrice(internalQos.getPrice() + compositionQos.getPrice());
			if (internalQos.getSecurity() <= compositionQos.getSecurity())
				externalQos.setSecurity(internalQos.getSecurity());
			else
				externalQos.setSecurity(compositionQos.getSecurity());
			externalQos.setReliability(internalQos.getReliability() * compositionQos.getReliability());
			this.setExternalQos(externalQos);
			return externalQos;
		}
	}

	public String getFoi() {
		return foi;
	}

	public Double getFrequency() {
		if (this.getEp() == null)
			return frequency;
		else if (this.getEp() != null) {
			return this.getEp().getFrequency();
		}
		return null;
	}

	// public void setExternalQos(QosVector externalQos) {
	// this.externalQos = externalQos;
	// }

	public String getnodeId() {
		return nodeId;
	}

	public Double getInternalFrequency() {
		return frequency;
	}

	public QosVector getInternalQos() {
		return internalQos;
	}

	public List<String> getPayloads() {
		return payloads;
	}

	public String getServiceId() {
		return serviceId;
	}

	public String getSrc() {
		return src;
	}

	public void print() throws CloneNotSupportedException, NodeRemovalException {
		System.out.println("ED: " + this.nodeId + ", " + this.eventType + ", " + " f:" + this.frequency);
		// + "Internal Qos: " + this.internalQos.toString() + "\n" + "External QoS: "
		// + this.getExternalQos().toString());
	}

	public void setEp(EventPattern ep) {
		this.ep = ep;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public void setFoi(String foi) {
		this.foi = foi;
	}

	public void setFrequency(Double frequency) {
		this.frequency = frequency;
	}

	public void setnodeId(String iD) {
		nodeId = iD;
	}

	public void setInternalQos(QosVector internalQos) {
		this.internalQos = internalQos;
	}

	public void setPayloads(List<String> payloads) {
		this.payloads = payloads;
	}

	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}

	public void setSrc(String src) {
		this.src = src;
	}

	public String toString() {
		String type = "";
		if (this.getEp() != null)
			type = this.getEp().getID();
		else
			type = this.getEventType();
		return "ED-" + this.nodeId + " = " + "SrvId:" + this.serviceId + " F:" + this.getInternalFrequency() + "; T:"
				+ type + " " + this.getInternalQos() + "; FOI: " + this.getFoi() + " Payloads: " + this.getPayloads();// +
																														// " \n Grounding:"
																														// +
		// this.getSrc();
		// return "ED: " + this.ID + ", " + this.eventType + ", f:" + this.frequency + ", "
		// + this.getExternalQos().toString();
		// return null;
	}

	public void updateInternalQos(QosVector newQos) {
		if (newQos.getAccuracy() > 0.0)
			this.getInternalQos().setAccuracy(newQos.getAccuracy());
		if (newQos.getLatency() > 0)
			this.getInternalQos().setLatency(newQos.getLatency());
		if (newQos.getPrice() > 0)
			this.getInternalQos().setPrice(newQos.getPrice());
		if (newQos.getReliability() > 0.0)
			this.getInternalQos().setReliability(newQos.getReliability());
		if (newQos.getSecurity() > 0)
			this.getInternalQos().setSecurity(newQos.getSecurity());
		if (newQos.getTraffic() > 0.0)
			this.getInternalQos().setTraffic(newQos.getTraffic());

	}

	public void updateExternalQos(QosVector newQos) throws CloneNotSupportedException, NodeRemovalException {
		QosVector currentExternalQos = this.getExternalQos();
		if (newQos.getAccuracy() > 0.0)
			currentExternalQos.setAccuracy(newQos.getAccuracy());
		if (newQos.getLatency() > 0)
			currentExternalQos.setLatency(newQos.getLatency());
		if (newQos.getPrice() > 0)
			currentExternalQos.setPrice(newQos.getPrice());
		if (newQos.getReliability() > 0.0)
			currentExternalQos.setReliability(newQos.getReliability());
		if (newQos.getSecurity() > 0)
			currentExternalQos.setSecurity(newQos.getSecurity());
		if (newQos.getTraffic() > 0.0)
			currentExternalQos.setTraffic(newQos.getTraffic());
		this.externalQos = currentExternalQos;

	}

	// public void setDistance(String distance) {
	// this.distance = distance;
	//
	// }
	//
	// public String getDistance() {
	// return this.distance;
	// }
}
