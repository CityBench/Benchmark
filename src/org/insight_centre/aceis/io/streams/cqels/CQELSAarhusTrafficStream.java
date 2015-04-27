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
import org.insight_centre.aceis.io.streams.DataWrapper;
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

	public CQELSAarhusTrafficStream(ExecContext context, String uri, String txtFile, EventDeclaration ed)
			throws IOException {
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

	@Override
	protected SensorObservation createObservation(Object objData) {
		SensorObservation so = DataWrapper.getAarhusTrafficObservation((CsvReader) objData, ed);
		DataWrapper.waitForInterval(currentObservation, so, startDate, getRate());
		this.currentObservation = so;
		return so;
	}

	@Override
	protected List<Statement> getStatements(SensorObservation data) throws NumberFormatException, IOException {
		return DataWrapper.getAarhusTrafficStatements((AarhusTrafficObservation) data, ed);
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