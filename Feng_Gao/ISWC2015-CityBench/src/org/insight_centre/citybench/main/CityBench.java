package org.insight_centre.citybench.main;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.deri.cqels.engine.CQELSEngine;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.ReasonerContext;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.EventRepository;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.cqels.CQELSAarhusParkingStream;
import org.insight_centre.aceis.io.streams.cqels.CQELSAarhusPollutionStream;
import org.insight_centre.aceis.io.streams.cqels.CQELSAarhusTrafficStream;
import org.insight_centre.aceis.io.streams.cqels.CQELSAarhusWeatherStream;
import org.insight_centre.aceis.io.streams.cqels.CQELSLocationStream;
import org.insight_centre.aceis.io.streams.cqels.CQELSResultListener;
import org.insight_centre.aceis.io.streams.cqels.CQELSSensorStream;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLAarhusPollutionStream;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLAarhusParkingStream;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLAarhusTrafficStream;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLAarhusWeatherStream;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLLocationStream;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLResultObserver;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLSensorStream;
//import org.insight_centre.aceis.io.streams.csparql.CSPARQLResultObserver;
import org.slf4j.Logger;
//import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.reasoner.Reasoner;
import com.hp.hpl.jena.reasoner.ReasonerRegistry;

import eu.larkc.csparql.engine.CsparqlEngineImpl;
import eu.larkc.csparql.engine.CsparqlQueryResultProxy;

import java.nio.file.Files;
import java.nio.file.Paths;

public class CityBench {
	public static ExecContext cqelsContext, tempContext;
	public static CsparqlEngineImpl csparqlEngine;
	private static final Logger logger = LoggerFactory.getLogger(CityBench.class);
	HashMap<String, String> parameters;
	Properties prop;
	int queryDuplicates = 1;
	private Map<String, String> queryMap = new HashMap<String, String>();
	private Set<String> registeredQueries = new HashSet<String>();
	private Set<String> startedStreams = new HashSet<String>();
	EventRepository er;
	private double rate = 1.0; // stream rate factor
	private long duration = 0; // experiment time in milliseconds
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private Date start, end;
	private double frequency = 1.0;

	// DatasetTDB
	/**
	 * @param prop
	 * @param parameters
	 *            Acceptable params: rates=(double)x, queryDuplicates=(int)y, duration=(long)z, startDate=(date in the
	 *            format of "yyyy-MM-dd'T'HH:mm:ss")a, endDate=b, frequency=(double)c. Start and end dates are
	 *            mandatory.
	 * @throws Exception
	 */
	public CityBench(Properties prop, HashMap<String, String> parameters) throws Exception {
		this.prop = prop;
		this.parameters = parameters;
		if (this.prop.get("dataset") != null) {
			tempContext = RDFFileManager.initializeCQELSContext(this.prop.get("dataset") + "",
					ReasonerRegistry.getRDFSReasoner());
			er = RDFFileManager.buildRepoFromFile(0);
		} else
			throw new Exception("Cannot load dataset.");
		if (this.parameters.containsKey("rate")) {
			this.rate = Double.parseDouble(this.parameters.get("rate"));
		}
		if (this.parameters.containsKey("duration")) {
			String durationStr = this.parameters.get("duration");
			String valueStr = durationStr.substring(0, durationStr.length() - 1);
			if (durationStr.contains("s"))
				duration = Integer.parseInt(valueStr) * 1000;
			else if (durationStr.contains("m"))
				duration = Integer.parseInt(valueStr) * 60000;
			else
				throw new Exception("Duration specification invalid.");
		}
		if (this.parameters.containsKey("queryDuplicates"))
			this.queryDuplicates = Integer.parseInt(this.parameters.get("queryDuplicates"));
		if (this.parameters.containsKey("startDate"))
			this.start = sdf.parse(this.parameters.get("startDate"));
		else
			throw new Exception("Start date not specified");
		if (this.parameters.containsKey("endDate"))
			this.end = sdf.parse(this.parameters.get("endDate"));
		else
			throw new Exception("End date not specified");
		if (this.parameters.containsKey("frequency"))
			this.frequency = Double.parseDouble(this.parameters.get("frequency"));

		logger.info("Parameters loaded: rate - " + this.rate + ", duration - " + this.duration + ", duplicates - "
				+ this.queryDuplicates);

		// this.startTest();
	}

	public void deployQuery(String qid, String query) {
		try {
			if (this.csparqlEngine != null) {
				this.startCSPARQLStreamsFromQuery(query);
				this.registerCSPARQLQuery(qid, query);
			} else {
				this.startCQELSStreamsFromQuery(query);
				this.registerCQELSQuery(qid, query);
			}

		} catch (ParseException e) {
			logger.info("Error parsing query: " + qid);
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * parse the stream URIs in the queries and identify the stream file locations
	 * 
	 * @return list of stream files
	 */
	List<String> getStreamFileNames() {
		Set<String> resultSet = new HashSet<String>();
		for (Entry en : this.queryMap.entrySet()) {
			try {
				resultSet.addAll(this.getStreamFileNamesFromQuery(en.getValue() + ""));
			} catch (Exception e) {
				logger.error("Error trying to get stream files.");
				e.printStackTrace();
			}
		}
		List<String> results = new ArrayList<String>();
		results.addAll(resultSet);
		return results;

	}

	List<String> getStreamFileNamesFromQuery(String query) throws Exception {
		Set<String> resultSet = new HashSet<String>();
		String[] streamSegments = query.trim().split("stream");
		if (streamSegments.length == 1)
			throw new Exception("Error parsing query, no stream statements found for: " + query);
		else {
			for (int i = 1; i < streamSegments.length; i++) {
				int indexOfLeftBracket = streamSegments[i].trim().indexOf("<");
				int indexOfRightBracket = streamSegments[i].trim().indexOf(">");
				String streamURI = streamSegments[i].substring(indexOfLeftBracket + 2, indexOfRightBracket + 1);
				logger.info("Stream detected: " + streamURI);
				resultSet.add(streamURI.split("#")[1] + ".stream");
			}
		}

		List<String> results = new ArrayList<String>();
		results.addAll(resultSet);
		return results;
	}

	private void initCQELS() throws Exception {
		cqelsContext = tempContext;
		this.startCQELSStreams();
		for (int i = 0; i < this.queryDuplicates; i++)
			this.registerCQELSQueries();
	}

	private void initCSPARQL() throws IOException {
		try {
			csparqlEngine = new CsparqlEngineImpl();
			csparqlEngine.initialize(true);
			this.startCSPARQLStreams();
			for (int i = 0; i < this.queryDuplicates; i++)
				this.registerCSPARQLQueries();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private List<String> loadQueries() throws Exception {
		String qd = this.prop.getProperty("query");
		if (qd != null) {
			File queryDirectory = new File(qd);
			if (!queryDirectory.exists())
				throw new Exception("Query directory not exist. " + qd);
			else if (!queryDirectory.isDirectory())
				throw new Exception("Query path specified is not a directory.");
			else {
				File[] queryFiles = queryDirectory.listFiles();
				if (queryFiles != null) {
					for (File queryFile : queryFiles) {
						String qid = queryFile.getName().split("\\.")[0];
						String qStr = new String(Files.readAllBytes(java.nio.file.Paths.get(queryDirectory
								+ File.separator + queryFile.getName())));
						if (this.prop.get("engine") != null && this.prop.get("engine").equals("csparql"))
							qStr = "REGISTER QUERY " + qid + " AS " + qStr;
						this.queryMap.put(qid, qStr);
					}
				} else
					throw new Exception("Cannot find query files.");
			}
		} else
			throw new Exception("Query directory not specified;");
		return null;
	}

	private void registerCQELSQueries() {
		for (Entry en : this.queryMap.entrySet()) {
			String qid = "Query-" + en.getKey() + UUID.randomUUID();
			String query = en.getValue() + "";
			this.registerCQELSQuery(qid, query);
		}
	}

	private void registerCQELSQuery(String qid, String query) {
		if (!this.registeredQueries.contains(qid)) {
			CQELSResultListener crl = new CQELSResultListener(qid);
			logger.info("Registering result observer: " + crl.getUri());
			ContinuousSelect cs = cqelsContext.registerSelect(query);
			cs.register(crl);
			this.registeredQueries.add(qid);
		}

	}

	private void registerCSPARQLQueries() throws ParseException {
		for (Entry en : this.queryMap.entrySet()) {
			String qid = "Query-" + en.getKey() + UUID.randomUUID();
			String query = en.getValue() + "";
			this.registerCSPARQLQuery(qid, query);
		}
	}

	private void registerCSPARQLQuery(String qid, String query) throws ParseException {
		if (!this.registeredQueries.contains(qid)) {
			CsparqlQueryResultProxy cqrp = csparqlEngine.registerQuery(query);
			CSPARQLResultObserver cro = new CSPARQLResultObserver(qid);
			logger.info("Registering result observer: " + cro.getIRI());
			csparqlEngine.registerStream(cro);

			// RDFStreamFormatter cro = new RDFStreamFormatter(streamURI);
			cqrp.addObserver(cro);
			this.registeredQueries.add(qid);
		}
	}

	private void startCQELSStreams() throws Exception {
		for (String s : this.queryMap.values()) {
			this.startCQELSStreamsFromQuery(s);
		}

	}

	private void startCQELSStreamsFromQuery(String query) throws Exception {
		List<String> streamNames = this.getStreamFileNamesFromQuery(query);
		for (String sn : streamNames) {
			String uri = RDFFileManager.defaultPrefix + sn.split("\\.")[0];
			String path = this.prop.getProperty("streams") + "/" + sn;
			if (!this.startedStreams.contains(uri)) {
				this.startedStreams.add(uri);
				CQELSSensorStream css;
				EventDeclaration ed = er.getEds().get(uri);
				if (ed.getEventType().contains("traffic")) {
					css = new CQELSAarhusTrafficStream(cqelsContext, uri, path, ed, start, end);
				} else if (ed.getEventType().contains("pollution")) {
					css = new CQELSAarhusPollutionStream(cqelsContext, uri, path, ed, start, end);
				} else if (ed.getEventType().contains("weather")) {
					css = new CQELSAarhusWeatherStream(cqelsContext, uri, path, ed, start, end);
				} else if (ed.getEventType().contains("location"))
					css = new CQELSLocationStream(cqelsContext, uri, path, ed);
				else if (ed.getEventType().contains("parking"))
					css = new CQELSAarhusParkingStream(cqelsContext, uri, path, ed, start, end);
				else
					throw new Exception("Sensor type not supported: " + ed.getEventType());
				css.setRate(this.rate);
				css.setFreq(this.frequency);
				new Thread(css).start();
			}
		}

	}

	private void startCSPARQLStreams() throws Exception {
		for (String s : this.queryMap.values()) {
			this.startCSPARQLStreamsFromQuery(s);
		}

	}

	private void startCSPARQLStreamsFromQuery(String query) throws Exception {
		List<String> streamNames = this.getStreamFileNamesFromQuery(query);
		for (String sn : streamNames) {
			String uri = RDFFileManager.defaultPrefix + sn.split("\\.")[0];
			String path = this.prop.getProperty("streams") + "/" + sn;
			if (!this.startedStreams.contains(uri)) {
				this.startedStreams.add(uri);
				CSPARQLSensorStream css;
				EventDeclaration ed = er.getEds().get(uri);
				if (ed.getEventType().contains("traffic")) {
					css = new CSPARQLAarhusTrafficStream(uri, path, ed, start, end);
				} else if (ed.getEventType().contains("pollution")) {
					css = new CSPARQLAarhusPollutionStream(uri, path, ed, start, end);
				} else if (ed.getEventType().contains("weather")) {
					css = new CSPARQLAarhusWeatherStream(uri, path, ed, start, end);
				} else if (ed.getEventType().contains("location"))
					css = new CSPARQLLocationStream(uri, path, ed);
				else if (ed.getEventType().contains("parking"))
					css = new CSPARQLAarhusParkingStream(uri, path, ed, start, end);
				else
					throw new Exception("Sensor type not supported.");
				css.setRate(this.rate);
				css.setFreq(this.frequency);
				csparqlEngine.registerStream(css);
				new Thread(css).start();
			}
		}

	}

	protected void startTest() throws Exception {
		logger.info(prop + "");
		logger.info(parameters + "");
		if (this.parameters.get("queryDuplicates") != null) {
			queryDuplicates = Integer.parseInt(this.parameters.get("queryDuplicates"));
		}
		if (prop.getProperty("engine") != null) {
			// load queries from query directory, each file contains 1 query
			this.loadQueries();
			if (prop.getProperty("engine").equals("cqels"))
				// start cqels test
				this.initCQELS();
			else if (prop.getProperty("engine").equals("csparql"))
				this.initCSPARQL();
			else
				throw new Exception("RSP engine not supported.");
		} else
			throw new Exception("RSP engine not specified.");

	}
}
