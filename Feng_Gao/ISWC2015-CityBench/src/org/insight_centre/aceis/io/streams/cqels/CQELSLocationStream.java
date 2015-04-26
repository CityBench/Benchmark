package org.insight_centre.aceis.io.streams.cqels;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.deri.cqels.engine.ExecContext;
//import org.insight.engine.ContextualFilteringManager;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.observations.SensorObservation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.vocabulary.RDF;

public class CQELSLocationStream extends CQELSSensorStream implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(CQELSLocationStream.class);
	private String txtFile;
	private EventDeclaration ed;

	// private ContextualFilteringManager cfm;

	public CQELSLocationStream(ExecContext context, String uri, String txtFile, EventDeclaration ed) {
		super(context, uri);
		this.txtFile = txtFile;
		this.ed = ed;

	}

	// public UserLocationStream(ExecContext context, String uri, String txtFile, EventDeclaration ed,
	// ContextualFilteringManager cfm) {
	// super(context, uri);
	// this.txtFile = txtFile;
	// this.ed = ed;
	// this.cfm = cfm;
	//
	// }

	@Override
	public void run() {
		logger.info("Starting sensor stream: " + this.getURI());
		try {
			if (txtFile.contains("Location")) {
				BufferedReader reader = new BufferedReader(new FileReader(txtFile));
				String strLine;
				while ((strLine = reader.readLine()) != null && (!stop)) {

					List<Statement> stmts = this.getStatements(this.createObservation(strLine));
					long messageByte = 0;
					for (Statement st : stmts) {
						if (st.getSubject().asNode() != null && st.getPredicate().asNode() != null
								&& st.getObject().asNode() != null) {
							stream(st.getSubject().asNode(), st.getPredicate().asNode(), st.getObject().asNode());
							logger.debug(this.getURI() + " Streaming: " + st.toString());
							messageByte += st.toString().getBytes().length;
							// fw.write(st.toString())

						}
					}
					// this.messageCnt += 1;
					// this.byteCnt += messageByte;
					// cw.write(new SimpleDateFormat("hh:mm:ss").format(new Date()));
					// cw.write(this.messageCnt + "");
					// cw.write(this.byteCnt + "");
					// cw.endRecord();
					// cw.flush();
					// stream(n(RDFFileManager.upPrefix + "007"), n(RDFFileManager.ctPrefix + "detectedAt"),
					// n(RDFFileManager.defaultPrefix + "FoI-c6edd705-66a5-45c1-9d93-220da78f9421"));
					if (sleep > 0) {
						try {
							Thread.sleep(sleep);
						} catch (InterruptedException e) {

							e.printStackTrace();

						}
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected List<Statement> getStatements(SensorObservation so) throws NumberFormatException, IOException {

		// String str = so.toString();
		String userStr = so.getFoi();
		String coordinatesStr = so.getValue().toString();
		Model m = ModelFactory.createDefaultModel();
		Double lat = Double.parseDouble(coordinatesStr.split(",")[0]);
		Double lon = Double.parseDouble(coordinatesStr.split(",")[1]);
		Resource serviceID = m.createResource(this.getURI());
		//
		// Resource user = m.createResource(userStr);

		Resource observation = m.createResource("Observation-" + UUID.randomUUID());
		observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));
		// observation.addProperty(RDF.type, m.createResource(RDFFileManager.saoPrefix + "StreamData"));

		// Resource location = m.createResource(this.getURI() + "LocationProperty-" + UUID.randomUUID());
		// location.addProperty(RDF.type, m.createResource(RDFFileManager.ctPrefix + "Location"));

		// Literal coordinates = m.createLiteral(lat + "," + lon);
		// coordinates.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasLatitude"), lat);
		// coordinates.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasLongtitude"), lon);

		// observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "featureOfInterest"), user);
		// observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"), location);
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);

		observation.addLiteral(m.createProperty(RDFFileManager.saoPrefix + "hasValue"), lat + "," + lon);
		// System.out.println("transformed: " + m.listStatements().toList().size());
		return m.listStatements().toList();
	}

	@Override
	protected SensorObservation createObservation(Object data) {
		String str = data.toString();
		String userStr = str.split("\\|")[0];
		String coordinatesStr = str.split("\\|")[1];
		SensorObservation so = new SensorObservation();
		so.setFoi(userStr);
		// so.setServiceId(this.getURI());
		so.setValue(coordinatesStr);
		so.setObTimeStamp(new Date());
		this.currentObservation = so;
		return so;
	}

}
