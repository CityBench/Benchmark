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
import org.insight_centre.aceis.eventmodel.TrafficReportService;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.DataWrapper;
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
				if (ed instanceof TrafficReportService)
					((TrafficReportService) ed).setDistance(Integer.parseInt(distance));

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
				if (ed instanceof TrafficReportService)
					((TrafficReportService) ed).setDistance(Integer.parseInt(distance));

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

	// private void annotateFoI(Model m, Resource observation, AarhusTrafficObservation data) {
	// Resource foi = m.createResource(RDFFileManager.defaultPrefix + "FoI-" + UUID.randomUUID());
	// foi.addProperty(m.createProperty(RDFFileManager.ctPrefix + "hasFirstNode"), this.annotateNode(m, 1, data));
	// foi.addProperty(m.createProperty(RDFFileManager.ctPrefix + "hasFirstNode"), this.annotateNode(m, 2, data));
	// observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "featureOfInterest"), foi);
	//
	// }

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

	@Override
	protected SensorObservation createObservation(Object objData) {
		SensorObservation so = DataWrapper.getAarhusTrafficObservation((CsvReader) objData, ed);
		DataWrapper.waitForInterval(currentObservation, so, startDate, getRate());
		this.currentObservation = so;
		return so;
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
		return DataWrapper.getAarhusTrafficStatements((AarhusTrafficObservation) data, ed);
	}

	public boolean isForJWSTest() {
		return forJWSTest;
	}

	public void run() {
		logger.info("Starting sensor stream: " + this.getIRI() + " " + this.startDate + ", " + this.endDate
				+ " distance: " + ((TrafficReportService) this.ed).getDistance());
		// logger.info("EventDeclaration: " + this.ed);
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

				AarhusTrafficObservation data = (AarhusTrafficObservation) this.createObservation(streamData);
				List<Statement> stmts = this.getStatements(data);
				long messageByte = 0;
				for (Statement st : stmts) {
					final RdfQuadruple q = new RdfQuadruple(st.getSubject().toString(), st.getPredicate().toString(),
							st.getObject().toString(), System.currentTimeMillis());
					this.put(q);
					logger.info(this.getIRI() + " Streaming: " + q.toString());
					messageByte += st.toString().getBytes().length;
				}

				this.messageCnt += 1;
				this.byteCnt += messageByte;
				logger.debug("Messages streamed to CSPARQL successfully.");
				if (sleep > 0) {
					try {
						if (this.getRate() != 1.0)
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