package org.insight_centre.aceis.eventmodel;

import java.util.List;

public class TrafficReportService extends EventDeclaration {
	private String reportId, node1Name, node2Name, node1Street, node1City, node2Street, node2City;
	private int distance;

	public int getDistance() {
		return distance;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	public String getReportId() {
		return reportId;
	}

	public void setReportId(String reportId) {
		this.reportId = reportId;
	}

	private Double node1Lat, node1Lon, node2Lat, node2Lon;
	private String node1StreetNo, node2StreetNo;

	public TrafficReportService(String iD, String src, String eventType, EventPattern ep, List<String> payloads,
			Double frequency) {
		super(iD, src, eventType, ep, payloads, frequency);

	}
	
	public String getNode1Name() {
		return node1Name;
	}

	public void setNode1Name(String node1Name) {
		this.node1Name = node1Name;
	}

	public String getNode2Name() {
		return node2Name;
	}

	public void setNode2Name(String node2Name) {
		this.node2Name = node2Name;
	}

	public String getNode1Street() {
		return node1Street;
	}

	public void setNode1Street(String node1Street) {
		this.node1Street = node1Street;
	}

	public String getNode1City() {
		return node1City;
	}

	public void setNode1City(String node1City) {
		this.node1City = node1City;
	}

	public String getNode2Street() {
		return node2Street;
	}

	public void setNode2Street(String node2Street) {
		this.node2Street = node2Street;
	}

	public String getNode2City() {
		return node2City;
	}

	public void setNode2City(String node2City) {
		this.node2City = node2City;
	}

	public Double getNode1Lat() {
		return node1Lat;
	}

	public void setNode1Lat(Double node1Lat) {
		this.node1Lat = node1Lat;
	}

	public Double getNode1Lon() {
		return node1Lon;
	}

	public void setNode1Lon(Double node1Lon) {
		this.node1Lon = node1Lon;
	}

	public Double getNode2Lat() {
		return node2Lat;
	}

	public void setNode2Lat(Double node2Lat) {
		this.node2Lat = node2Lat;
	}

	public Double getNode2Lon() {
		return node2Lon;
	}

	public void setNode2Lon(Double node2Lon) {
		this.node2Lon = node2Lon;
	}

	public String getNode1StreetNo() {
		return node1StreetNo;
	}

	public void setNode1StreetNo(String node1StreetNo) {
		this.node1StreetNo = node1StreetNo;
	}

	public String getNode2StreetNo() {
		return node2StreetNo;
	}

	public void setNode2StreetNo(String node2StreetNo) {
		this.node2StreetNo = node2StreetNo;
	}

	public String toString() {
		return super.toString();
		// + "Report ID: " + reportId + "\n Node1: " + node1Name + " lat:" + node1Lat + " lon:"
		// + node1Lon + " loc:" + node1StreetNo + "," + node1Street + "," + node1City + "\n" + " Node2: "
		// + node2Name + " lat:" + node2Lat + " lon:" + node2Lon + " loc:" + node2StreetNo + "," + node2Street
		// + "," + node2City + "\n";
	}
}
