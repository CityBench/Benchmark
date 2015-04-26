package org.insight_centre.aceis.io.rdf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.Filter;
import org.insight_centre.aceis.eventmodel.NodeRemovalException;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.eventmodel.TrafficReportService;
import org.insight_centre.aceis.io.EventRepository;

public class TrafficDataManager {

	public static List<TrafficReportService> readTrafficData(String filePath) throws IOException {
		File file = new File(filePath);
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		String totalStr = br.readLine();
		int start = totalStr.lastIndexOf("records") + 11;
		int end = totalStr.lastIndexOf("]");
		String dataStr = totalStr.substring(start, end);
		String[] records = dataStr.split("}");
		List<TrafficReportService> trss = new ArrayList<TrafficReportService>();
		for (String s : records) {
			String node1StreetNo = "", node2StreetNo = "", node1Name = "", node2Name = "", node1Street = "", node2Street = "", node1City = "", node2City = "", reportId = "", id = "";
			int distance = -1;
			Double node1Lat = 0.0, node1Lon = 0.0, node2Lat = 0.0, node2Lon = 0.0;
			String[] typeVals = s.split(",");
			for (String typeVal : typeVals) {
				if (typeVal.length() < 2)
					continue;
				// System.out.println(typeVal);
				String type = typeVal.split(":")[0];
				String val = typeVal.split(":")[1].replaceAll("\"", "").replaceAll(" ", "");
				if (type.contains("id\"")) {

					id = val;
				} else if (type.contains("REPORT_ID\""))
					reportId = val;
				else if (type.contains("DISTANCE_IN_METERS"))
					distance = Integer.parseInt(val);
				else if (type.contains("POINT_1_STREET\""))
					node1Street = val;
				else if (type.contains("POINT_1_LAT\""))
					node1Lat = Double.parseDouble(val);
				else if (type.contains("POINT_1_LNG\""))
					node1Lon = Double.parseDouble(val);
				else if (type.contains("POINT_1_NAME\""))
					node1Name = val;
				else if (type.contains("POINT_1_CITY\""))
					node1City = val;
				else if (type.contains("POINT_1_STREET_NUMBER\""))
					node1StreetNo = val;

				else if (type.contains("POINT_2_STREET\""))
					node2Street = val;
				else if (type.contains("POINT_2_LAT\""))
					node2Lat = Double.parseDouble(val);
				else if (type.contains("POINT_2_LNG\""))
					node2Lon = Double.parseDouble(val);
				else if (type.contains("POINT_2_NAME\""))
					node2Name = val;
				else if (type.contains("POINT_2_CITY\""))
					node2City = val;
				else if (type.contains("POINT_2_STREET_NUMBER\""))
					node2StreetNo = val;
			}
			List<String> payloads = new ArrayList<String>();
			String foiStr = RDFFileManager.defaultPrefix + "FoI-" + UUID.randomUUID();
			payloads.add(RDFFileManager.ctPrefix + "VehicleCount|" + foiStr + "|" + RDFFileManager.defaultPrefix
					+ "Property-" + UUID.randomUUID());
			payloads.add(RDFFileManager.ctPrefix + "MeasureTime|" + foiStr + "|" + RDFFileManager.defaultPrefix
					+ "Property-" + UUID.randomUUID());
			payloads.add(RDFFileManager.ctPrefix + "AvgSpeed|" + foiStr + "|" + RDFFileManager.defaultPrefix
					+ "Property-" + UUID.randomUUID());
			payloads.add(RDFFileManager.ctPrefix + "CongestionLevel|" + foiStr + "|" + RDFFileManager.defaultPrefix
					+ "Property-" + UUID.randomUUID());
			payloads.add(RDFFileManager.ctPrefix + "EstimatedTime|" + foiStr + "|" + RDFFileManager.defaultPrefix
					+ "Property-" + UUID.randomUUID());

			TrafficReportService trs = new TrafficReportService(
					RDFFileManager.defaultPrefix + id,
					"http://ckan.projects.cavi.dk/api/action/datastore_search_sql?sql=SELECT%20*%20from%20%22d7e6c54f-dc2a-4fae-9f2a-b036c804837d%22%20WHERE%20'REPORT_ID'%20=%20'"
							+ reportId + "'", "traffic_report", null, payloads, 5.0);
			// trs.setNode1City(node1City);
			trs.setNode1Lat(node1Lat);
			trs.setNode1Lon(node1Lon);
			// trs.setNode1Name(node1Name);
			// trs.setNode1Street(node1Street);
			// trs.setNode1StreetNo(node1StreetNo);
			//
			// trs.setNode2City(node2City);
			trs.setNode2Lat(node2Lat);
			trs.setNode2Lon(node2Lon);
			// trs.setNode2Name(node2Name);
			// trs.setNode2Street(node2Street);
			// trs.setNode2StreetNo(node2StreetNo);
			trs.setFoi(node1Lat + "," + node1Lon + "-" + node2Lat + "," + node2Lon);
			trs.setDistance(distance);
			trs.setReportId(reportId);
			trs.setServiceId(trs.getnodeId());
			// trs.setInternalQos(new QosVector(50, 5, 4, 0.9, 0.9));
			// trs.setFoi(RDFFileManager.osmPrefix + "Node-" + id);
			trss.add(trs);
		}
		System.out.println(records.length);
		for (TrafficReportService trs : trss)
			System.out.println(trs.toString());
		return trss;
	}

	public static void main(String[] args) throws IOException, NodeRemovalException {
		List<TrafficReportService> trs = TrafficDataManager.readTrafficData("dataset/TrafficSensors.txt");
		EventRepository er = new EventRepository();
		int cnt = 0;
		for (TrafficReportService tr : trs) {
			er.getEds().put(tr.getnodeId(), tr);
			String foiId = RDFFileManager.defaultPrefix + "FoI-" + UUID.randomUUID();
			Double lat = tr.getNode1Lat();
			Double lon = tr.getNode1Lon();
			String foiStr = lat + "," + lon + "-" + lat + "," + lon;
			List<String> pollutionPayloads = new ArrayList<String>();
			pollutionPayloads.add(RDFFileManager.ctPrefix + "API|" + foiId + "|" + RDFFileManager.defaultPrefix
					+ "Property-" + UUID.randomUUID());
			// pollutionPayloads.add(RDFFileManager.ctPrefix + "PM2_5|" + foiId + "|" + RDFFileManager.defaultPrefix
			// + "Property-" + UUID.randomUUID());
			// pollutionPayloads.add(RDFFileManager.ctPrefix + "PM10|" + foiId + "|" + RDFFileManager.defaultPrefix
			// + "Property-" + UUID.randomUUID());
			EventDeclaration pollutionSensor = new EventDeclaration(RDFFileManager.defaultPrefix + "pollutionData"
					+ tr.getReportId(), null, "air_pollution", null, pollutionPayloads, 5.0);
			pollutionSensor.setFoi(foiStr);
			pollutionSensor.setServiceId(pollutionSensor.getnodeId());
			er.getEds().put(pollutionSensor.getnodeId(), pollutionSensor);
			cnt += 1;
		}
		List<String> weatherPayloads = new ArrayList<String>();
		String weatherFoiId = RDFFileManager.defaultPrefix + "FoI-" + UUID.randomUUID();
		String foiStr = "56.1,10.1-56.1,10.1";
		weatherPayloads.add(RDFFileManager.ctPrefix + "WindSpeed|" + weatherFoiId + "|" + RDFFileManager.defaultPrefix
				+ "Property-" + UUID.randomUUID());
		weatherPayloads.add(RDFFileManager.ctPrefix + "Humidity|" + weatherFoiId + "|" + RDFFileManager.defaultPrefix
				+ "Property-" + UUID.randomUUID());
		weatherPayloads.add(RDFFileManager.ctPrefix + "Temperature|" + weatherFoiId + "|"
				+ RDFFileManager.defaultPrefix + "Property-" + UUID.randomUUID());
		EventDeclaration weatherSensor = new EventDeclaration(RDFFileManager.defaultPrefix + "weather-0", null,
				"weather_report", null, weatherPayloads, 5.0);
		weatherSensor.setFoi(foiStr);
		weatherSensor.setServiceId(weatherSensor.getnodeId());
		er.getEds().put(weatherSensor.getnodeId(), weatherSensor);
		RDFFileManager.writeRepoToFile("Scenario1Sensors.n3", er);
	}

	public static EventDeclaration createCongestionEvent(String id, EventDeclaration tr, BigDecimal bigDecimal) {
		EventPattern ep = new EventPattern();

		EventDeclaration result = new EventDeclaration(id, tr.getSrc() + "Congest", "complex", ep, null, null,
				tr.getInternalQos());
		ep.setID("EP-" + result.getnodeId());
		ep.getEds().add(tr);
		Map<String, List<Filter>> filterMap = new HashMap<String, List<Filter>>();
		List<Filter> filters = new ArrayList<Filter>();
		filters.add(new Filter(RDFFileManager.cesPrefix + "CongestionLevel", bigDecimal, Filter.geq));
		filterMap.put(tr.getnodeId(), filters);
		ep.setFilters(filterMap);
		return result;
	}
}
