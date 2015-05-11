package org.insight_centre.citybench.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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
import org.insight_centre.aceis.observations.SensorObservation;
import org.insight_centre.aceis.utils.test.PerformanceMonitor;
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
	public enum RSPEngine {
		cqels, csparql
	}

	public static ExecContext cqelsContext, tempContext;
	public static CsparqlEngineImpl csparqlEngine;
	private static final Logger logger = LoggerFactory.getLogger(CityBench.class);
	public static ConcurrentHashMap<String, SensorObservation> obMap = new ConcurrentHashMap<String, SensorObservation>();

	// HashMap<String, String> parameters;
	// Properties prop;

	public static void main(String[] args) {
		try {
			Properties prop = new Properties();
			// logger.info(Main.class.getClassLoader().);
			File in = new File("citybench.properties");
			FileInputStream fis = new FileInputStream(in);
			prop.load(fis);
			fis.close();
			// Thread.
			HashMap<String, String> parameters = new HashMap<String, String>();
			for (String s : args) {
				parameters.put(s.split("=")[0], s.split("=")[1]);
			}
			CityBench cb = new CityBench(prop, parameters);
			cb.startTest();
			// BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			// System.out.print("Please press a key to stop the server.");
			// reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (Exception e) {
			// logger.error(e.getMessage());
			e.printStackTrace();
			System.exit(0);
		}
	}

	private String dataset, ontology, cqels_query, csparql_query, streams;
	private long duration = 0; // experiment time in milliseconds
	private RSPEngine engine;
	EventRepository er;
	private double frequency = 1.0;
	public static PerformanceMonitor pm;
	private List<String> queries;
	int queryDuplicates = 1;
	private Map<String, String> queryMap = new HashMap<String, String>();

	// public Map<String, String> getQueryMap() {
	// return queryMap;
	// }

	private double rate = 1.0; // stream rate factor
	public static ConcurrentHashMap<String, Object> registeredQueries = new ConcurrentHashMap<String, Object>();
	public static List startedStreamObjects = new ArrayList();
	private String resultName;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private Date start, end;
	private Set<String> startedStreams = new HashSet<String>();

	// private double rate,frequency
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
		// parse configuration file
		// this.parameters = parameters;
		try {
			this.dataset = prop.getProperty("dataset");
			this.ontology = prop.getProperty("ontology");
			this.cqels_query = prop.getProperty("cqels_query");
			this.csparql_query = prop.getProperty("csparql_query");
			this.streams = prop.getProperty("streams");
			if (this.dataset == null || this.ontology == null || this.cqels_query == null || this.csparql_query == null
					|| this.streams == null)
				throw new Exception("Configuration properties incomplete.");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);

		}

		// parse parameters
		if (parameters.containsKey("query")) {
			this.queries = Arrays.asList(parameters.get("query").split(","));
		}
		if (parameters.get("queryDuplicates") != null) {
			queryDuplicates = Integer.parseInt(parameters.get("queryDuplicates"));
		}
		if (parameters.containsKey("engine")) {
			if (parameters.get("engine").equals("cqels"))
				this.engine = RSPEngine.cqels;
			else if (parameters.get("engine").equals("csparql"))
				this.engine = RSPEngine.csparql;
			else
				throw new Exception("RSP Engine not supported.");
		} else
			throw new Exception("RSP Engine not specified.");

		if (parameters.containsKey("rate")) {
			this.rate = Double.parseDouble(parameters.get("rate"));
		}
		if (parameters.containsKey("duration")) {
			String durationStr = parameters.get("duration");
			String valueStr = durationStr.substring(0, durationStr.length() - 1);
			if (durationStr.contains("s"))
				duration = Integer.parseInt(valueStr) * 1000;
			else if (durationStr.contains("m"))
				duration = Integer.parseInt(valueStr) * 60000;
			else
				throw new Exception("Duration specification invalid.");
		}
		if (parameters.containsKey("queryDuplicates"))
			this.queryDuplicates = Integer.parseInt(parameters.get("queryDuplicates"));
		if (parameters.containsKey("startDate"))
			this.start = sdf.parse(parameters.get("startDate"));
		else
			throw new Exception("Start date not specified");
		if (parameters.containsKey("endDate"))
			this.end = sdf.parse(parameters.get("endDate"));
		else
			throw new Exception("End date not specified");
		if (parameters.containsKey("frequency"))
			this.frequency = Double.parseDouble(parameters.get("frequency"));

		logger.info("Parameters loaded: engine - " + this.engine + ", queries - " + this.queries + ", rate - "
				+ this.rate + ", frequency - " + this.frequency + ", duration - " + this.duration + ", duplicates - "
				+ this.queryDuplicates + ", start - " + this.start + ", end - " + this.end);

		this.resultName = UUID.randomUUID() + " r=" + this.rate + ",f=" + this.frequency + ",dup="
				+ this.queryDuplicates + ",e=" + this.engine + ",q=" + this.queries;// +
		// parameters.toString();
		// initialize datasets
		try {
			tempContext = RDFFileManager.initializeCQELSContext(this.dataset, ReasonerRegistry.getRDFSReasoner());
			er = RDFFileManager.buildRepoFromFile(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
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

	public String getResultName() {
		return resultName;
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

		for (int i = 0; i < this.queryDuplicates; i++)
			this.registerCQELSQueries();
		this.startCQELSStreams();
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
		String qd;
		if (this.engine == RSPEngine.cqels)
			qd = this.cqels_query;
		else
			qd = this.csparql_query;
		if (this.queries == null) {
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
						if (this.engine != null && this.engine == RSPEngine.csparql)
							qStr = "REGISTER QUERY " + qid + " AS " + qStr;
						this.queryMap.put(qid, qStr);
					}
				} else
					throw new Exception("Cannot find query files.");
			}
		} else {
			for (String qid : this.queries) {
				try {

					File queryFile = new File(qd + File.separator + qid);
					String qStr = new String(Files.readAllBytes(queryFile.toPath()));
					qid = qid.split("\\.")[0];
					if (this.engine != null && this.engine == RSPEngine.csparql)
						qStr = "REGISTER QUERY " + qid + " AS " + qStr;
					this.queryMap.put(qid, qStr);
				} catch (Exception e) {
					logger.error("Could not load query file.");
					e.printStackTrace();
				}
			}
		}
		// throw new Exception("Query directory not specified;");
		return null;
	}

	private void registerCQELSQueries() {
		for (Entry en : this.queryMap.entrySet()) {
			String qid = en.getKey() + "-" + UUID.randomUUID();
			String query = en.getValue() + "";
			registerCQELSQuery(qid, query);
		}
	}

	private void registerCQELSQuery(String qid, String query) {
		if (!registeredQueries.keySet().contains(qid)) {
			CQELSResultListener crl = new CQELSResultListener(qid);
			logger.info("Registering result observer: " + crl.getUri());
			ContinuousSelect cs = cqelsContext.registerSelect(query);
			cs.register(crl);
			registeredQueries.put(qid, crl);
		}

	}

	private void registerCSPARQLQueries() throws ParseException {
		for (Entry en : this.queryMap.entrySet()) {
			String qid = en.getKey() + "-" + UUID.randomUUID();
			String query = en.getValue() + "";
			registerCSPARQLQuery(qid, query);
		}
	}

	private void registerCSPARQLQuery(String qid, String query) throws ParseException {
		if (!registeredQueries.keySet().contains(qid)) {
			CsparqlQueryResultProxy cqrp = csparqlEngine.registerQuery(query);
			CSPARQLResultObserver cro = new CSPARQLResultObserver(qid);
			logger.info("Registering result observer: " + cro.getIRI());
			csparqlEngine.registerStream(cro);

			// RDFStreamFormatter cro = new RDFStreamFormatter(streamURI);
			cqrp.addObserver(cro);
			registeredQueries.put(qid, cro);
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
			String path = this.streams + "/" + sn;
			if (!this.startedStreams.contains(uri)) {
				this.startedStreams.add(uri);
				CQELSSensorStream css;
				EventDeclaration ed = er.getEds().get(uri);
				if (ed == null)
					throw new Exception("ED not found for: " + uri);
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
				startedStreamObjects.add(css);
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
			String path = this.streams + "/" + sn;
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
				startedStreamObjects.add(css);
			}
		}

	}

	protected void startTest() throws Exception {
		// load queries from query directory, each file contains 1 query
		this.loadQueries();
		pm = new PerformanceMonitor(queryMap, duration, queryDuplicates, resultName);
		new Thread(pm).start();
		if (this.engine == RSPEngine.cqels)
			// start cqels test
			this.initCQELS();
		else if (this.engine == RSPEngine.csparql)
			this.initCSPARQL();

	}
}
