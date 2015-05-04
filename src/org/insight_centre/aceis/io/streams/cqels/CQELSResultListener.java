package org.insight_centre.aceis.io.streams.cqels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;
import org.insight_centre.citybench.main.CityBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.core.Var;

import eu.larkc.csparql.common.RDFTuple;

public class CQELSResultListener implements ContinuousListener {
	private String uri;
	private static final Logger logger = LoggerFactory.getLogger(CQELSResultListener.class);
	public static Set<String> capturedObIds = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
	public static Set<String> capturedResults = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

	public CQELSResultListener(String string) {
		setUri(string);
	}

	@Override
	public void update(Mapping mapping) {
		String result = "";
		try {
			Map<String, Long> latencies = new HashMap<String, Long>();
			// int cnt = 0;
			for (Iterator<Var> vars = mapping.vars(); vars.hasNext();) {
				Var var = vars.next();
				String varName = var.getName();
				String varStr = CityBench.cqelsContext.engine().decode(mapping.get(var)).toString();
				if (varName.contains("obId")) {
					if (!capturedObIds.contains(varStr)) {
						capturedObIds.add(varStr);
						long initTime = CityBench.obMap.get(varStr).getSysTimestamp().getTime();
						latencies.put(varStr, (System.currentTimeMillis() - initTime));
					}
				}
				result += " " + varStr;

			}
			// logger.info("CQELS result arrived: " + result);
			if (!capturedResults.contains(result)) {
				capturedResults.add(result);
				// uncomment for testing the completeness, i.e., showing how many observations are captured
				// logger.info("CQELS result arrived " + capturedResults.size() + ", obs size: " + capturedObIds.size()
				// + ", result: " + result);
				CityBench.pm.addResults(getUri(), latencies, 1);
			} else {
				logger.debug("CQELS result discarded: " + result);
			}

		} catch (Exception e) {
			logger.error("CQELS decoding error: " + e.getMessage());
			e.printStackTrace();
		}

	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

}
