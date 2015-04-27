package org.insight_centre.aceis.io.streams.cqels;

import java.io.IOException;
import java.util.Iterator;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;
import org.insight_centre.citybench.main.CityBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.core.Var;

public class CQELSResultListener implements ContinuousListener {
	private String uri;
	private static final Logger logger = LoggerFactory.getLogger(CQELSResultListener.class);

	public CQELSResultListener(String string) {
		setUri(string);
	}

	@Override
	public void update(Mapping mapping) {
		String result = "";
		try {
			for (Iterator<Var> vars = mapping.vars(); vars.hasNext();)
				result += " " + CityBench.cqelsContext.engine().decode(mapping.get(vars.next()));
		} catch (Exception e) {
			logger.error("CQELS decoding error: " + e.getMessage());
		}
		logger.info("CQELS result arrived: " + result);

	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

}
