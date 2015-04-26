package org.insight_centre.aceis.io.streams.cqels;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;

public class CQELSResultListener implements ContinuousListener {
	private String uri;

	public CQELSResultListener(String string) {
		setUri(string);
	}

	@Override
	public void update(Mapping arg0) {
		// TODO Auto-generated method stub

	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

}
