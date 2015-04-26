package org.insight_centre.aceis.io.streams.csparql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;
//import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.cqels.CQELSAarhusPollutionStream;
import org.insight_centre.aceis.observations.AarhusTrafficObservation;
import org.insight_centre.aceis.observations.SensorObservation;
//import org.insight_centre.aceis.utils.test.Simulator2.QosSimulationMode;
//import org.insight_centre.aceis.utils.test.jwsTest.JWSTest;
//import org.insight_centre.aceis.utils.test.jwsTest.JWSUtils;
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

import eu.larkc.csparql.cep.api.RdfQuadruple;

public class CSPARQLAarhusTrafficStream extends CSPARQLSensorStream implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(CSPARQLAarhusTrafficStream.class);
	static long time1;
	EventDeclaration ed;
	private boolean forJWSTest = false;
	private List<String> lines = new ArrayList<String>();
	private long messageCnt, byteCnt;
	String p1Street, p1City, p1Lat, p1Lon, p2Street, p2City, p2Lat, p2Lon, p1Country, p2Country, distance, id;
	// private QosSimulationMode qosSimulationMode = QosSimulationMode.none;
	// long sleep = 1000; // default frequency is 1.0
	// boolean stop = false;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd-k-m-s");
	private Date startDate = null, endDate = null;
	CsvReader streamData, metaData;
	private List<String> subscribers = new ArrayList<String>();
	String txtFile;
	private int cnt = 0;

	public CSPARQLAarhusTrafficStream(String uri, String txtFile, EventDeclaration ed) throws IOException {
		super(uri);
		String fileName = "";
		if (this.getIRI().split("#").length > 1)
			fileName = this.getIRI().split("#")[1];
		else
			fileName = this.getIRI();
		// logFile = new File("resultlog/" + fileName + ".csv");
		// fw = new FileWriter(logFile);
		// cw = new CsvWriter(fw, ',');
		// cw.write("time");
		// cw.write("message_cnt");
		// cw.write("byte_cnt");
		// cw.endRecord();heh
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

	public CSPARQLAarhusTrafficStream(String uri, String txtFile, EventDeclaration ed, Date start, Date end)
			throws IOException {
		super(uri);
		logger.info("IRI: " + this.getIRI().split("#")[1] + ed.getInternalQos());
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

	public synchronized void addSubscriber(String s) {
		this.subscribers.add(s);
	}

	private void annotateFoI(Model m, Resource observation, AarhusTrafficObservation data) {
		Resource foi = m.createResource(RDFFileManager.defaultPrefix + "FoI-" + UUID.randomUUID());
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
		// String obId = RDFFileManager.defaultPrefix + "Observation-" + this.getIRI().split("#")[1] + this.cnt;
		String obId = data.getObId();
		Resource observation = m.createResource(obId);
		data.setObId(observation.toString());
		// System.out.println("OB: " + observation.toString());
		observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));
		// observation.addProperty(RDF.type, m.createResource(RDFFileManager.saoPrefix + "StreamData"));
		annotateSensor(m, observation, data);
		annotateProperty(m, observation, pStr);
		// annotateFoI(m, observation, data);
		annotateValue(m, observation, pStr, data);

		// if (this.forJWSTest) {
		// // System.out.println("putting obid: " + data.getObId());
		// JWSTest.obMap.put(data.getObId(), data);
		// }
	}

	private void annotateProperty(Model m, Resource observation, String observedProperty) {
		// Resource property = m.createResource(observedProperty.split("\\|")[2]);
		// property.addProperty(RDF.type, m.createResource(observedProperty.split("\\|")[0]));
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"),
				m.createResource(observedProperty.split("\\|")[0]));

	}

	private void annotateSensor(Model m, Resource observation, AarhusTrafficObservation data) {
		Resource serviceID = m.createResource(this.getIRI());
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
		else if (observedProperty.toString().contains("VehicleCount")) {
			double value = data.getVehicle_count();
			// if (this.qosSimulationMode == QosSimulationMode.accuracy) {
			// if (Math.random() >= this.ed.getInternalQos().getAccuracy()) {
			// value = Math.random() * 20;
			// logger.info(this.getIRI().split("#")[1] + "Creating false value: " + value);
			// }
			// }
			observation.addLiteral(hasValue, value);
		} else if (observedProperty.toString().contains("MeasuredTime"))
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
			AarhusTrafficObservation data;
			if (!this.txtFile.contains("mean"))
				data = new AarhusTrafficObservation(Double.parseDouble(streamData.get("REPORT_ID")),
						Double.parseDouble(streamData.get("avgSpeed")), Double.parseDouble(streamData
								.get("vehicleCount")), Double.parseDouble(streamData.get("avgMeasuredTime")), 0, 0,
						null, null, 0.0, 0.0, null, null, 0.0, 0.0, null, null, streamData.get("TIMESTAMP"));
			else
				data = new AarhusTrafficObservation(0.0, Math.random(), Math.random(), 0.0, 0, 0, null, null, 0.0, 0.0,
						null, null, 0.0, 0.0, null, null, sdf.format(sdf2.parse((streamData.get("startTime")))));
			logger.debug(this.getIRI() + ": streaming record for " + data.getObTimeStamp() + " @"
					+ data.getSysTimestamp());
			// data.setObTimeStamp(Date.parse(data.getTimestamp()));
			if (!this.txtFile.contains("mean")) {
				Double distance = Double.parseDouble(this.distance);
				data.setEstimatedTime(distance / data.getAverageSpeed());
				data.setCongestionLevel(data.getVehicle_count() / distance);
			}
			data.setObId(RDFFileManager.defaultPrefix + "Observation-" + this.getIRI().split("#")[1] + this.cnt);
			this.currentObservation = data;
			return data;
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			System.out.println("error parsing time.");
			e.printStackTrace();
		}
		return null;
	}

	public Date getEndDate() {
		return endDate;
	}

	// public QosSimulationMode getQosSimulationMode() {
	// return qosSimulationMode;
	// }

	public Date getStartDate() {
		return startDate;
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
		else {
			// test purposes only
			// this.annotateObservation(m, m.createResource(RDFFileManager.ctPrefix + "AvgSpeed").toString(),
			// (AarhusTrafficObservation) data);
			this.annotateObservation(m, m.createResource(RDFFileManager.ctPrefix + "VehicleCount").toString(),
					(AarhusTrafficObservation) data);
		}
		return m.listStatements().toList();
	}

	public boolean isForJWSTest() {
		return forJWSTest;
	}

	public void run() {
		logger.info("Starting sensor stream: " + this.getIRI() + " " + this.startDate + ", " + this.endDate);
		try {
			// Reads csv document for traffic metadata

			while (streamData.readRecord() && !stop) {
				cnt += 1;
				Date obTime;

				if (!this.txtFile.contains("mean"))
					obTime = sdf.parse(streamData.get("TIMESTAMP"));
				else
					obTime = sdf2.parse(streamData.get("startTime"));
				// logger.info("obTime: " + obTime);
				logger.debug("Reading data: " + streamData.toString());
				if (this.startDate != null && this.endDate != null) {
					if (obTime.before(this.startDate) || obTime.after(this.endDate)) {
						logger.debug(this.getIRI() + ": Disgarded observation observed at: " + obTime);
						continue;
					}
				}
				if (this.forJWSTest && !this.txtFile.contains("mean")) {
					Double avgSpd = Double.parseDouble(streamData.get("avgSpeed"));
					Double vehicleCnt = Double.parseDouble(streamData.get("vehicleCount"));
					// skip inconsistent cnt and avgspeed
					if (vehicleCnt == 0.0 && avgSpd > 0.0) {
						logger.info("Skipping corrupted record.");
						continue;
					}
				}
				AarhusTrafficObservation data = (AarhusTrafficObservation) this.createObservation(streamData);
				// ACEISEngine.getSubscriptionManager().getObMap().put(data.getObId(), data);
				// data.get
				// if (this.qosSimulationMode == QosSimulationMode.completeness) {
				// if (Math.random() >= this.ed.getInternalQos().getReliability()) {
				// try {
				// Thread.sleep(sleep);
				// } catch (InterruptedException e) {
				// e.printStackTrace();
				// }
				// continue;
				// }
				// }
				// stream(n(RDFFileManager.defaultPrefix + id), n(RDFFileManager.ctPrefix + "hasETA"),
				// n(data.getEstimatedTime() + ""));
				// System.out.println("Streaming: " + RDFFileManager.defaultPrefix
				// + streamData.get("REPORT_ID"));
				List<Statement> stmts = this.getStatements(data);
				long messageByte = 0;
				// if (this.qosSimulationMode == QosSimulationMode.latency) {
				// // if (Math.random() >= this.ed.getInternalQos().()) {
				// try {
				// Thread.sleep(this.ed.getInternalQos().getLatency());
				// } catch (InterruptedException e) {
				// e.printStackTrace();
				// }
				// }
				for (Statement st : stmts) {
					final RdfQuadruple q = new RdfQuadruple(st.getSubject().toString(), st.getPredicate().toString(),
							st.getObject().toString(), System.currentTimeMillis());
					this.put(q);
					logger.debug(this.getIRI() + " Streaming: " + q.toString());
					messageByte += st.toString().getBytes().length;
				}

				this.messageCnt += 1;
				this.byteCnt += messageByte;
				logger.debug("Messages streamed to CSPARQL successfully.");
				// if (ACEISEngine.initialized()) {
				// ACEISEngine.getSubscriptionManager().getStreamCurrentTimeMap()
				// .put(this.getIRI(), data.getObTimeStamp());
				// for (String subscriber : this.subscribers) {
				// // logger.info("adding consumed for: " + subscriber);
				// ACEISEngine.getSubscriptionManager().addConsumedMsgCnt(subscriber);
				// }
				// }
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
			logger.info("Stream Terminated: " + this.getIRI() + " total bytes sent: " + this.byteCnt);
			this.stop();
			// if (this.forJWSTest) {
			// JWSTest.terminatedStreams.add(this.getIRI());
			// JWSTest.inBytesMap.put(this.getIRI(), this.byteCnt);
			// // System.out.println("Streams terminated: " + JWSTest.terminatedStreams.size());
			// }
		}
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	public void setForJWSTest(boolean forJWSTest) {
		this.forJWSTest = forJWSTest;
	}

	// public void setQosSimulationMode(QosSimulationMode qosSimulationMode) {
	// this.qosSimulationMode = qosSimulationMode;
	// }

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

}