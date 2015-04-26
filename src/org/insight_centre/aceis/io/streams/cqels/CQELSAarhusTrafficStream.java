package org.insight_centre.aceis.io.streams.cqels;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.observations.AarhusTrafficObservation;
import org.insight_centre.aceis.observations.SensorObservation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import com.hp.hpl.jena.rdf.model.Literal;
//import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.vocabulary.RDF;

public class CQELSAarhusTrafficStream extends CQELSSensorStream implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(CQELSAarhusTrafficStream.class);
	static long time1;
	// private CsvWriter cw;
	EventDeclaration ed;
	// private FileWriter fw;
	private File logFile;
	private Date startDate = null, endDate = null;
	private long messageCnt, byteCnt;
	String p1Street, p1City, p1Lat, p1Lon, p2Street, p2City, p2Lat, p2Lon, p1Country, p2Country, distance, id;
	// long sleep = 1000; // default frequency is 1.0
	// boolean stop = false;
	CsvReader streamData, metaData;
	String txtFile;
	private List<String> lines = new ArrayList<String>();

	// private Map<String, Integer> messageCnt;
	//
	// public Map<String, Integer> getMessageCnt() {
	// return messageCnt;
	// }
	//
	// public void setMessageCnt(Map<String, Integer> messageCnt) {
	// this.messageCnt = messageCnt;
	// }

	public CQELSAarhusTrafficStream(ExecContext context, String uri, String txtFile, EventDeclaration ed) throws IOException {
		super(context, uri);
		String fileName = "";
		if (this.getURI().split("#").length > 1)
			fileName = this.getURI().split("#")[1];
		else
			fileName = this.getURI();
		// logFile = new File("resultlog/" + fileName + ".csv");
		// fw = new FileWriter(logFile);
		// cw = new CsvWriter(fw, ',');
		// cw.write("time");
		// cw.write("message_cnt");
		// cw.write("byte_cnt");
		// cw.endRecord();
		// cw.flush();
		messageCnt = 0;
		byteCnt = 0;
		this.txtFile = txtFile;
		this.ed = ed;
		// time1 = time.getTime();
		streamData = new CsvReader(String.valueOf(txtFile));
		streamData.setTrimWhitespace(false);
		streamData.setDelimiter(',');
		streamData.readHeaders();
		// streamData.skipRecord();
		metaData = new CsvReader("dataset/MetaData/trafficMetaData.csv");
		metaData.readHeaders();
		streamData.readRecord();
		while (metaData.readRecord()) {
			if (streamData.get("REPORT_ID").equals(metaData.get("REPORT_ID"))) {
				// p1Street = metaData.get("POINT_1_STREET");
				// p1City = metaData.get("POINT_1_CITY");
				// p1Lat = metaData.get("POINT_1_LAT");
				// p1Lon = metaData.get("POINT_1_LNG");
				// p1Country = metaData.get("POINT_2_COUNTRY");
				// p2Street = metaData.get("POINT_2_STREET");
				// p2City = metaData.get("POINT_2_CITY");
				// p2Lat = metaData.get("POINT_2_LAT");
				// p2Lon = metaData.get("POINT_2_LNG");
				// p2Country = metaData.get("POINT_2_COUNTRY");
				distance = metaData.get("DISTANCE_IN_METERS");
				// timestamp = metaData.get("TIMESTAMP");
				// id = metaData.get("extID");
				// stream(n(RDFFileManager.defaultPrefix + streamData.get("REPORT_ID")),
				// n(RDFFileManager.ctPrefix + "hasETA"), n(data.getEstimatedTime() + ""));
				// System.out.println("metadata: " + metaData.toString());
				metaData.close();
				break;
			}
		}
	}

	public CQELSAarhusTrafficStream(ExecContext context, String uri, String txtFile, EventDeclaration ed, Date start,
			Date end) throws IOException {
		super(context, uri);
		this.startDate = start;
		this.endDate = end;
		messageCnt = 0;
		byteCnt = 0;
		this.txtFile = txtFile;
		this.ed = ed;
		streamData = new CsvReader(String.valueOf(txtFile));
		streamData.setTrimWhitespace(false);
		streamData.setDelimiter(',');
		streamData.readHeaders();
		metaData = new CsvReader("dataset/MetaData/trafficMetaData.csv");
		metaData.readHeaders();
		streamData.readRecord();
		while (metaData.readRecord()) {
			if (streamData.get("REPORT_ID").equals(metaData.get("REPORT_ID"))) {
				// p1Street = metaData.get("POINT_1_STREET");
				// p1City = metaData.get("POINT_1_CITY");
				// p1Lat = metaData.get("POINT_1_LAT");
				// p1Lon = metaData.get("POINT_1_LNG");
				// p1Country = metaData.get("POINT_2_COUNTRY");
				// p2Street = metaData.get("POINT_2_STREET");
				// p2City = metaData.get("POINT_2_CITY");
				// p2Lat = metaData.get("POINT_2_LAT");
				// p2Lon = metaData.get("POINT_2_LNG");
				// p2Country = metaData.get("POINT_2_COUNTRY");
				distance = metaData.get("DISTANCE_IN_METERS");
				// timestamp = metaData.get("TIMESTAMP");
				// id = metaData.get("extID");
				metaData.close();
				break;
			}
		}
	}

	private void annotateFoI(Model m, Resource observation, AarhusTrafficObservation data) {
		Resource foi = m.createResource("FoI-" + UUID.randomUUID());
		foi.addProperty(m.createProperty(RDFFileManager.ctPrefix + "hasFirstNode"), this.annotateNode(m, 1, data));
		foi.addProperty(m.createProperty(RDFFileManager.ctPrefix + "hasFirstNode"), this.annotateNode(m, 2, data));
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "featureOfInterest"), foi);

	}

	private Resource annotateNode(Model m, int index, AarhusTrafficObservation data) {
		Resource node;
		String city, street;
		Double lat, lon;
		if (index == 1) {
			city = data.getCity_1();
			street = data.getStreet1();
			lat = data.getLatitude1();
			lon = data.getLongtitude1();
		} else {
			city = data.getCity_2();
			street = data.getStreet2();
			lat = data.getLatitude2();
			lon = data.getLongtitude2();
		}
		node = m.createResource().addProperty(RDF.type, m.createResource(RDFFileManager.ctPrefix + "Node"));
		node.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasStreet"), street);
		node.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasCity"), city);
		node.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasLatitude"), lat);
		node.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasLongtitude"), lon);
		return node;

	}

	// public ExecContext getContext() {
	// return this.
	// }
	private void annotateObservation(Model m, String pStr, AarhusTrafficObservation data) {
		Resource observation = m.createResource("Observation-" + UUID.randomUUID());
		// System.out.println("OB: " + observation.toString());
		observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));
		// observation.addProperty(RDF.type, m.createResource(RDFFileManager.saoPrefix + "StreamData"));
		annotateSensor(m, observation, data);
		annotateProperty(m, observation, pStr);
		// annotateFoI(m, observation, data);
		annotateValue(m, observation, pStr, data);

	}

	private void annotateProperty(Model m, Resource observation, String observedProperty) {
		// Resource property = m.createResource(observedProperty.split("\\|")[2]);
		// property.addProperty(RDF.type, m.createResource(observedProperty.split("\\|")[0]));
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"),
				m.createResource(observedProperty.split("\\|")[0]));

	}

	private void annotateSensor(Model m, Resource observation, AarhusTrafficObservation data) {
		Resource serviceID = m.createResource(this.getURI());
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);

	}

	private void annotateValue(Model m, Resource observation, String observedProperty, AarhusTrafficObservation data) {
		// Resource value = m.createResource();
		// observation.addProperty(m.createProperty(RDFFileManager.saoPrefix + "value"), value);
		Property hasValue = m.createProperty(RDFFileManager.saoPrefix + "hasValue");
		// Literal l;
		// System.out.println("Annotating: " + observedProperty.toString());
		if (observedProperty.contains("AvgSpeed"))
			observation.addLiteral(hasValue, data.getAverageSpeed());
		else if (observedProperty.toString().contains("VehicleCount"))
			observation.addLiteral(hasValue, data.getVehicle_count());
		else if (observedProperty.toString().contains("MeasuredTime"))
			observation.addLiteral(hasValue, data.getAvgMeasuredTime());
		else if (observedProperty.toString().contains("EstimatedTime"))
			observation.addLiteral(hasValue, data.getEstimatedTime());
		else if (observedProperty.toString().contains("CongestionLevel"))
			observation.addLiteral(hasValue, data.getCongestionLevel());
	}

	@Override
	protected SensorObservation createObservation(Object objData) {
		try {
			CsvReader streamData = (CsvReader) objData;
			AarhusTrafficObservation data = new AarhusTrafficObservation(
					Double.parseDouble(streamData.get("REPORT_ID")), Double.parseDouble(streamData.get("avgSpeed")),
					Double.parseDouble(streamData.get("vehicleCount")), Double.parseDouble(streamData
							.get("avgMeasuredTime")), 0, 0, null, null, 0.0, 0.0, null, null, 0.0, 0.0, null, null,
					streamData.get("TIMESTAMP"));
			logger.debug(this.getURI() + ": streaming record @" + data.getObTimeStamp());
			// data.setObTimeStamp(Date.parse(data.getTimestamp()));
			Double distance = Double.parseDouble(this.distance);
			data.setEstimatedTime(distance / data.getAverageSpeed());
			data.setCongestionLevel(data.getVehicle_count() / distance);
			this.currentObservation = data;
			return data;
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	protected List<Statement> getStatements(SensorObservation data) throws NumberFormatException, IOException {
		// StreamData data = ReadDocument.getStreamData(file);
		Model m = ModelFactory.createDefaultModel();
		if (ed != null)
			for (String s : ed.getPayloads()) {
				// if (s.contains("EstimatedTime")) {
				// Resource observedProperty = m.createResource(s);
				this.annotateObservation(m, s, (AarhusTrafficObservation) data);
				// break;
				// }
			}
		else
			// test purposes only
			this.annotateObservation(m, m.createResource(RDFFileManager.ctPrefix + "EstimatedTime").toString(),
					(AarhusTrafficObservation) data);
		return m.listStatements().toList();
	}

	public void run() {
		logger.info("Starting sensor stream: " + this.getURI() + " " + this.startDate + ", " + this.endDate);
		try {
			// Reads csv document for traffic metadata

			while (streamData.readRecord() && !stop) {
				Date obTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(streamData.get("TIMESTAMP"));
				logger.debug("Reading data: " + streamData.toString());
				if (this.startDate != null && this.endDate != null) {
					if (obTime.before(this.startDate) || obTime.after(this.endDate)) {
						logger.debug(this.getURI() + ": Disgarded observation @" + obTime);
						continue;
					}
				}
				AarhusTrafficObservation data = (AarhusTrafficObservation) this.createObservation(streamData);
				// stream(n(RDFFileManager.defaultPrefix + id), n(RDFFileManager.ctPrefix + "hasETA"),
				// n(data.getEstimatedTime() + ""));
				// System.out.println("Streaming: " + RDFFileManager.defaultPrefix
				// + streamData.get("REPORT_ID"));
				List<Statement> stmts = this.getStatements(data);
				long messageByte = 0;
				for (Statement st : stmts) {
					stream(st.getSubject().asNode(), st.getPredicate().asNode(), st.getObject().asNode());
					logger.debug(this.getURI() + " Streaming: " + st.toString());
					messageByte += st.toString().getBytes().length;
				}
				this.messageCnt += 1;
				this.byteCnt += messageByte;
				logger.info("Messages streamed to CQELS successfully.");
				// cw.write(new SimpleDateFormat("hh:mm:ss").format(new Date()));
				// cw.write(this.messageCnt + "");
				// cw.write(this.byteCnt + "");
				// cw.endRecord();
				// cw.flush();
				// 2914
				// metaData.close();
				if (sleep > 0) {
					try {
						Thread.sleep(sleep);
					} catch (InterruptedException e) {

						e.printStackTrace();

					}
				}
			}
		} catch (Exception e) {

			e.printStackTrace();
		} finally {
			logger.info("Stream Terminated: " + this.getURI());
			this.stop();
			// Thread.s
			// try {
			// cw.flush();
			// cw.close();
			// } catch (IOException e) {

			// e.printStackTrace();
			// }

		}
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	// public void setRate(int rate) {
	// sleep = sleep / rate;// should be 1000
	// }

}