package org.insight_centre.aceis.io.streams;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.TrafficReportService;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.observations.AarhusParkingObservation;
import org.insight_centre.aceis.observations.AarhusTrafficObservation;
import org.insight_centre.aceis.observations.PollutionObservation;
import org.insight_centre.aceis.observations.SensorObservation;
import org.insight_centre.aceis.observations.WeatherObservation;
import org.insight_centre.citybench.main.CityBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.vocabulary.RDF;

public class DataWrapper {
	private static final Logger logger = LoggerFactory.getLogger(DataWrapper.class);
	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");
	public static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

	public static void waitForInterval(SensorObservation previous, SensorObservation current, Date init, double rate) {
		if (rate != 1.0) {
			try {
				long previousTime = 0;
				if (previous != null)
					previousTime = previous.getObTimeStamp().getTime();
				else
					previousTime = init.getTime();
				// List<Statement> stmts = this.getStatements(po);

				// logger.info("previous: " + previousTime + ", current: " + current.getObTimeStamp().getTime());
				long ms = (long) ((current.getObTimeStamp().getTime() - previousTime));
				// logger.info("waiting..." + ms / rate);
				Thread.sleep((long) (ms / rate));
				current.setSysTimestamp(new Date());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static SensorObservation getAarhusTrafficObservation(CsvReader streamData, EventDeclaration ed) {
		try {
			// CsvReader streamData = (CsvReader) objData;
			AarhusTrafficObservation data;
			// if (!this.txtFile.contains("mean"))
			data = new AarhusTrafficObservation(Double.parseDouble(streamData.get("REPORT_ID")),
					Double.parseDouble(streamData.get("avgSpeed")), Double.parseDouble(streamData.get("vehicleCount")),
					Double.parseDouble(streamData.get("avgMeasuredTime")), 0, 0, null, null, 0.0, 0.0, null, null, 0.0,
					0.0, null, null, streamData.get("TIMESTAMP"));
			String obId = "AarhusTrafficObservation-" + streamData.get("_id");
			Double distance = Double.parseDouble(((TrafficReportService) ed).getDistance() + "");
			if (data.getAverageSpeed() != 0)
				data.setEstimatedTime(distance / data.getAverageSpeed());
			else
				data.setEstimatedTime(-1.0);
			if (distance != 0)
				data.setCongestionLevel(data.getVehicle_count() / distance);
			else
				data.setCongestionLevel(-1.0);
			data.setObId(obId);
			;
			// this.currentObservation = data;
			return data;
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static List<Statement> getAarhusTrafficStatements(AarhusTrafficObservation data, EventDeclaration ed) {
		Model m = ModelFactory.createDefaultModel();
		if (ed != null)
			for (String pStr : ed.getPayloads()) {
				// if (s.contains("EstimatedTime")) {
				// Resource observedProperty = m.createResource(s);
				String obId = data.getObId();
				Resource observation = m.createResource(RDFFileManager.defaultPrefix + obId + UUID.randomUUID());
				CityBench.obMap.put(observation.toString(), data);
				// data.setObId(observation.toString());
				// System.out.println("OB: " + observation.toString());
				observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));

				Resource serviceID = m.createResource(ed.getServiceId());
				observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);
				observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"),
						m.createResource(pStr.split("\\|")[2]));
				Property hasValue = m.createProperty(RDFFileManager.saoPrefix + "hasValue");
				// System.out.println("Annotating: " + observedProperty.toString());
				if (pStr.contains("AvgSpeed"))
					observation.addLiteral(hasValue, data.getAverageSpeed());
				else if (pStr.contains("VehicleCount")) {
					double value = data.getVehicle_count();
					observation.addLiteral(hasValue, value);
				} else if (pStr.contains("MeasuredTime"))
					observation.addLiteral(hasValue, data.getAvgMeasuredTime());
				else if (pStr.contains("EstimatedTime"))
					observation.addLiteral(hasValue, data.getEstimatedTime());
				else if (pStr.contains("CongestionLevel"))
					observation.addLiteral(hasValue, data.getCongestionLevel());
				// break;
				// }
			}
		return m.listStatements().toList();
		// return null;

	}

	public synchronized static SensorObservation getAarhusPollutionObservation(CsvReader streamData, EventDeclaration ed) {

		try {
			// CsvReader streamData = (CsvReader) data;
			int ozone = Integer.parseInt(streamData.get("ozone")), particullate_matter = Integer.parseInt(streamData
					.get("particullate_matter")), carbon_monoxide = Integer.parseInt(streamData.get("carbon_monoxide")), sulfure_dioxide = Integer
					.parseInt(streamData.get("sulfure_dioxide")), nitrogen_dioxide = Integer.parseInt(streamData
					.get("nitrogen_dioxide"));
			Date obTime = sdf.parse(streamData.get("timestamp"));
			PollutionObservation po = new PollutionObservation(0.0, 0.0, 0.0, ozone, particullate_matter,
					carbon_monoxide, sulfure_dioxide, nitrogen_dioxide, obTime);
			// logger.debug(ed.getServiceId() + ": streaming record @" + po.getObTimeStamp());
			po.setObId("AarhusPollutionObservation-" + (int) Math.random() * 10000);
			return po;
		} catch (NumberFormatException | IOException | ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static List<Statement> getAarhusPollutionStatement(SensorObservation so, EventDeclaration ed) {
		Model m = ModelFactory.createDefaultModel();
		if (ed != null)
			for (String s : ed.getPayloads()) {
				Resource observation = m
						.createResource(RDFFileManager.defaultPrefix + so.getObId() + UUID.randomUUID());
				// so.setObId(RDFFileManager.defaultPrefix + observation.toString());
				CityBench.obMap.put(observation.toString(), so);
				observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));

				Resource serviceID = m.createResource(ed.getServiceId());
				observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);
				observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"),
						m.createResource(s.split("\\|")[2]));
				Property hasValue = m.createProperty(RDFFileManager.saoPrefix + "hasValue");
				observation.addLiteral(hasValue, ((PollutionObservation) so).getApi());
			}
		return m.listStatements().toList();
	}

	public static SensorObservation getAarhusWeatherObservation(CsvReader streamData, EventDeclaration ed) {
		try {
			// CsvReader streamData = (CsvReader) data;
			int hum = Integer.parseInt(streamData.get("hum"));
			double tempm = Double.parseDouble(streamData.get("tempm"));
			double wspdm = Double.parseDouble(streamData.get("wspdm"));
			Date obTime = sdf2.parse(streamData.get("TIMESTAMP"));
			WeatherObservation wo = new WeatherObservation(tempm, hum, wspdm, obTime);
			logger.debug(ed.getServiceId() + ": streaming record @" + wo.getObTimeStamp());
			wo.setObId("AarhusWeatherObservation-" + (int) Math.random() * 1000);
			// this.currentObservation = wo;
			return wo;
		} catch (NumberFormatException | IOException | ParseException e) {
			e.printStackTrace();
		}
		return null;

	}

	public static List<Statement> getAarhusWeatherStatements(SensorObservation wo, EventDeclaration ed) {
		Model m = ModelFactory.createDefaultModel();
		if (ed != null)
			for (String s : ed.getPayloads()) {
				Resource observation = m
						.createResource(RDFFileManager.defaultPrefix + wo.getObId() + UUID.randomUUID());
				// wo.setObId(observation.toString());
				CityBench.obMap.put(observation.toString(), wo);
				observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));
				Resource serviceID = m.createResource(ed.getServiceId());
				observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);
				observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"),
						m.createResource(s.split("\\|")[2]));
				Property hasValue = m.createProperty(RDFFileManager.saoPrefix + "hasValue");
				if (s.contains("Temperature"))
					observation.addLiteral(hasValue, ((WeatherObservation) wo).getTemperature());
				else if (s.toString().contains("Humidity"))
					observation.addLiteral(hasValue, ((WeatherObservation) wo).getHumidity());
				else if (s.toString().contains("WindSpeed"))
					observation.addLiteral(hasValue, ((WeatherObservation) wo).getWindSpeed());
			}
		return m.listStatements().toList();
	}

	public static SensorObservation getUserLocationObservation(String data, EventDeclaration ed) {
		String str = data.toString();
		String userStr = str.split("\\|")[0];
		String coordinatesStr = str.split("\\|")[1];
		SensorObservation so = new SensorObservation();
		so.setFoi(userStr);
		// so.setServiceId(this.getURI());
		so.setValue(coordinatesStr);
		so.setObTimeStamp(new Date());
		so.setObId("UserLocationObservation-" + (int) Math.random() * 10000);
		return so;
	}

	public static List<Statement> getUserLocationStatements(SensorObservation so, EventDeclaration ed) {
		String userStr = so.getFoi();
		String coordinatesStr = so.getValue().toString();
		Model m = ModelFactory.createDefaultModel();
		double lat = Double.parseDouble(coordinatesStr.split(",")[0]);
		double lon = Double.parseDouble(coordinatesStr.split(",")[1]);
		Resource serviceID = m.createResource(ed.getServiceId());
		//
		// Resource user = m.createResource(userStr);

		Resource observation = m.createResource(RDFFileManager.defaultPrefix + so.getObId() + UUID.randomUUID());
		CityBench.obMap.put(observation.toString(), so);
		observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));
		// observation.addProperty(RDF.type, m.createResource(RDFFileManager.saoPrefix + "StreamData"));

		// location.addProperty(RDF.type, m.createResource(RDFFileManager.ctPrefix + "Location"));

		Resource coordinates = m.createResource();
		coordinates.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasLatitude"), lat);
		coordinates.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasLongitude"), lon);

		// observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "featureOfInterest"), user);
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"),
				m.createResource(ed.getPayloads().get(0).split("\\|")[2]));
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);
		// fake fixed foi
		observation
				.addProperty(
						m.createProperty(RDFFileManager.ssnPrefix + "featureOfInterest"),
						m.createResource("http://iot.ee.surrey.ac.uk/citypulse/datasets/aarhusculturalevents/culturalEvents_aarhus#context_do63jk2t8c3bjkfb119ojgkhs7"));

		observation.addProperty(m.createProperty(RDFFileManager.saoPrefix + "hasValue"), coordinates);
		// System.out.println("transformed: " + m.listStatements().toList().size());s
		return m.listStatements().toList();
	}

	public synchronized static SensorObservation getAarhusParkingObservation(CsvReader streamData, EventDeclaration ed) {
		try {
			// CsvReader streamData = (CsvReader) data;
			int vehicleCnt = Integer.parseInt(streamData.get("vehiclecount")), id = Integer.parseInt(streamData
					.get("_id")), total_spaces = Integer.parseInt(streamData.get("totalspaces"));
			String garagecode = streamData.get("garagecode");
			Date obTime = sdf.parse(streamData.get("updatetime"));
			AarhusParkingObservation apo = new AarhusParkingObservation(total_spaces - vehicleCnt, garagecode, "", 0.0,
					0.0);
			apo.setObTimeStamp(obTime);
			// logger.info("Annotating obTime: " + obTime + " in ms: " + obTime.getTime());
			apo.setObId("AarhusParkingObservation-" + id);
			logger.debug(ed.getServiceId() + ": streaming record @" + apo.getObTimeStamp());

			return apo;
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			logger.error("ed parse error: " + ed.getServiceId());
			e.printStackTrace();
		}
		return null;
	}

	public static List<Statement> getAarhusParkingStatements(SensorObservation so, EventDeclaration ed) {
		Model m = ModelFactory.createDefaultModel();
		Resource observation = m.createResource(RDFFileManager.defaultPrefix + so.getObId() + UUID.randomUUID());
		CityBench.obMap.put(observation.toString(), so);
		observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));
		// observation.addProperty(RDF.type,
		// m.createResource(RDFFileManager.saoPrefix + "StreamData"));
		Resource serviceID = m.createResource(ed.getServiceId());
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);
		// Resource property = m.createResource(s.split("\\|")[2]);
		// property.addProperty(RDF.type, m.createResource(s.split("\\|")[0]));
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"),
				m.createResource(ed.getPayloads().get(0).split("\\|")[2]));
		Property hasValue = m.createProperty(RDFFileManager.saoPrefix + "hasValue");
		// Literal l;
		// System.out.println("Annotating: " + observedProperty.toString());
		// if (observedProperty.contains("AvgSpeed"))
		observation.addLiteral(hasValue, ((AarhusParkingObservation) so).getVacancies());
		// observation.addLiteral(m.createProperty(RDFFileManager.ssnPrefix + "featureOfInterest"),
		// ((AarhusParkingObservation) so).getGarageCode());
		return m.listStatements().toList();
	}
	// private void annotateFoI(Model m, Resource observation, AarhusTrafficObservation data) {
	// Resource foi = m.createResource("FoI-" + UUID.randomUUID());
	// foi.addProperty(m.createProperty(RDFFileManager.ctPrefix + "hasFirstNode"), this.annotateNode(m, 1, data));
	// foi.addProperty(m.createProperty(RDFFileManager.ctPrefix + "hasFirstNode"), this.annotateNode(m, 2, data));
	// observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "featureOfInterest"), foi);
	//
	// }
	//
	// private Resource annotateNode(Model m, int index, AarhusTrafficObservation data) {
	// Resource node;
	// String city, street;
	// Double lat, lon;
	// if (index == 1) {
	// city = data.getCity_1();
	// street = data.getStreet1();
	// lat = data.getLatitude1();
	// lon = data.getLongtitude1();
	// } else {
	// city = data.getCity_2();
	// street = data.getStreet2();
	// lat = data.getLatitude2();
	// lon = data.getLongtitude2();
	// }
	// node = m.createResource().addProperty(RDF.type, m.createResource(RDFFileManager.ctPrefix + "Node"));
	// node.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasStreet"), street);
	// node.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasCity"), city);
	// node.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasLatitude"), lat);
	// node.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasLongtitude"), lon);
	// return node;
	//
	// }
}
