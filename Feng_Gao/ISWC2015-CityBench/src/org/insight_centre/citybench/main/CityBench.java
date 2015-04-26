package org.insight_centre.citybench.main;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
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
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.cqels.CQELSResultListener;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLResultObserver;
//import org.insight_centre.aceis.io.streams.csparql.CSPARQLResultObserver;
import org.slf4j.Logger;
//import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.reasoner.ReasonerRegistry;

import eu.larkc.csparql.engine.CsparqlEngineImpl;
import eu.larkc.csparql.engine.CsparqlQueryResultProxy;

import java.nio.file.Files;
import java.nio.file.Paths;

public class CityBench {
	private static final Logger logger = LoggerFactory.getLogger(CityBench.class);
	Properties prop;
	HashMap<String, String> parameters;
	public static ExecContext cqelsContext;
	public static CsparqlEngineImpl csparqlEngine;
	private Map<String, String> queryMap = new HashMap<String, String>();
	int queryDuplicates = 1;

	/**
	 * @param prop
	 * @param parameters
	 *            Acceptable params: rates=x, queryDuplicates=y, duration=z,
	 * @throws Exception
	 */
	public CityBench(Properties prop, HashMap<String, String> parameters) throws Exception {
		this.prop = prop;
		this.parameters = parameters;
		// this.startTest();
	}

	protected void startTest() throws Exception {
		logger.info(prop + "");
		logger.info(parameters + "");
		if (this.parameters.get("queryDuplicates") != null) {
			queryDuplicates = Integer.parseInt(this.parameters.get("queryDuplicates"));
		}
		if (prop.getProperty("engine") != null) {
			// load queries from query directory, each file contains 1 query
			this.LoadQueries();
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

	/**
	 * parse the stream URIs in the queries and identify the stream file locations
	 * 
	 * @return list of stream files
	 */
	List<String> getStreamFileNames() {
		Set<String> resultSet = new HashSet<String>();
		for (Entry en : this.queryMap.entrySet()) {
			String queryStr = en.getValue() + "";
		}
		List<String> results = new ArrayList<String>();
		results.addAll(resultSet);
		return results;

	}

	private void initCSPARQL() {
		try {
			csparqlEngine = new CsparqlEngineImpl();
			csparqlEngine.initialize(true);
			this.startCSPARQLStreams();
			for (int i = 0; i < this.queryDuplicates; i++)
				this.registerCSPARQLQueries();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void registerCSPARQLQueries() throws ParseException {
		for (Entry en : this.queryMap.entrySet()) {
			CsparqlQueryResultProxy cqrp = csparqlEngine.registerQuery(en.getValue() + "");
			CSPARQLResultObserver cro = new CSPARQLResultObserver("Query-" + en.getKey() + UUID.randomUUID());
			logger.info("Registering result observer: " + cro.getIRI());
			csparqlEngine.registerStream(cro);
			// RDFStreamFormatter cro = new RDFStreamFormatter(streamURI);
			cqrp.addObserver(cro);
		}
	}

	private List<String> LoadQueries() throws Exception {
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
						this.queryMap.put(queryFile.getName().split("\\.")[0],
								new String(Files.readAllBytes(java.nio.file.Paths.get(queryFile.getName()))));
					}
				} else
					throw new Exception("Cannot find query files.");
			}
		} else
			throw new Exception("Query directory not specified;");
		return null;
	}

	private void startCSPARQLStreams() {
		// TODO Auto-generated method stub

	}

	private void initCQELS() {
		cqelsContext = new ReasonerContext("CQELS_DB/", true, ReasonerRegistry.getRDFSReasoner());
		this.startCQELSStreams();
		for (int i = 0; i < this.queryDuplicates; i++)
			this.registerCQELSQueries();
	}

	private void registerCQELSQueries() {
		for (Entry en : this.queryMap.entrySet()) {
			CQELSResultListener crl = new CQELSResultListener("Query-" + en.getKey() + UUID.randomUUID());
			logger.info("Registering result observer: " + crl.getUri());
			ContinuousSelect cs = cqelsContext.registerSelect(en.getValue() + "");
			cs.register(crl);
		}
	}

	private void startCQELSStreams() {
		// TODO Auto-generated method stub

	}
}
