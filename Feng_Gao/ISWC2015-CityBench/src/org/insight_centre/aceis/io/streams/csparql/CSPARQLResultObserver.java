package org.insight_centre.aceis.io.streams.csparql;

//import org.insight_centre.aceis.engine.ACEISEngine;

//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.common.streams.format.GenericObservable;
import eu.larkc.csparql.engine.RDFStreamFormatter;

public class CSPARQLResultObserver extends RDFStreamFormatter {
	private static final Logger logger = LoggerFactory.getLogger(CSPARQLResultObserver.class);

	public CSPARQLResultObserver(String iri) {
		super(iri);
		// TODO Auto-generated constructor stub
	}

	public void update(final GenericObservable<RDFTable> observed, final RDFTable q) {
		// System.out.println();
		// System.out.println(this.getIRI() + "-------" + q.size() + " results at SystemTime=["
		// + System.currentTimeMillis() + "]--------");
		// Set<String> currentResults = new HashSet<String>();
		// logger.info(this.getIRI().split("#")[1] + ": " + q.size() + " updates received.");
		for (final RDFTuple t : q) {

			// t.g
			// System.out.println(t.toString());
			String result = t.toString().replaceAll("\t", " ").trim();

			String[] results = result.split(" ");

			// sendResultToEngine(results);
			logger.info(this.getIRI() + " Results: " + result);
		}

		// System.out.println();

	}

}
