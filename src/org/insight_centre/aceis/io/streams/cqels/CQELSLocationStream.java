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
import org.insight_centre.aceis.io.streams.DataWrapper;
import org.insight_centre.aceis.observations.SensorObservation;
import org.insight_centre.citybench.main.CityBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;
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
		String userStr = so.getFoi();
		String coordinatesStr = so.getValue().toString();
		Model m = ModelFactory.createDefaultModel();
		double lat = Double.parseDouble(coordinatesStr.split(",")[0]);
		double lon = Double.parseDouble(coordinatesStr.split(",")[1]);
		Resource serviceID = m.createResource(ed.getServiceId());
		//
		// Resource user = m.createResource(userStr);

		Resource observation = m.createResource(RDFFileManager.defaultPrefix + so.getObId() + UUID.randomUUID());
		CityBench.obMap.put(observation.toString(), so);
		observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));
		// observation.addProperty(RDF.type, m.createResource(RDFFileManager.saoPrefix + "StreamData"));

		// location.addProperty(RDF.type, m.createResource(RDFFileManager.ctPrefix + "Location"));

		Resource coordinates = m.createResource();
		coordinates.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasLatitude"), lat);
		coordinates.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasLongitude"), lon);

		// observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "featureOfInterest"), user);
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"),
				m.createResource(ed.getPayloads().get(0).split("\\|")[2]));
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);
		// fake fixed foi
		observation
				.addProperty(
						m.createProperty(RDFFileManager.ssnPrefix + "featureOfInterest"),
						m.createResource("http://iot.ee.surrey.ac.uk/citypulse/datasets/aarhusculturalevents/culturalEvents_aarhus#context_do63jk2t8c3bjkfb119ojgkhs7"));

		observation.addProperty(m.createProperty(RDFFileManager.saoPrefix + "hasValue"), coordinates);
		// System.out.println("transformed: " + m.listStatements().toList().size());s
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
		so.setObId("UserLocationObservation-" + (int) Math.random() * 10000);
		// return so;
		this.currentObservation = so;
		return so;
	}

}
