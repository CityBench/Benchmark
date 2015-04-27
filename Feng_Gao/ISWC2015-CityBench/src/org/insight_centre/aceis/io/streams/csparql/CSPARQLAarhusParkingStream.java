package org.insight_centre.aceis.io.streams.csparql;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.DataWrapper;
import org.insight_centre.aceis.observations.AarhusParkingObservation;
import org.insight_centre.aceis.observations.PollutionObservation;
import org.insight_centre.aceis.observations.SensorObservation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;
//import com.google.gson.Gson;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.vocabulary.RDF;

import eu.larkc.csparql.cep.api.RdfQuadruple;

public class CSPARQLAarhusParkingStream extends CSPARQLSensorStream implements Runnable {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	CsvReader streamData;
	EventDeclaration ed;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");
	private Date startDate = null;
	private Date endDate = null;

	public static void main(String[] args) {
		try {
			List<String> payloads = new ArrayList<String>();
			payloads.add(RDFFileManager.defaultPrefix + "Property-1|" + RDFFileManager.defaultPrefix + "FoI-1|"
					+ RDFFileManager.ctPrefix + "ParkingVacancy");
			EventDeclaration ed = new EventDeclaration("testEd", "testsrc", "air_pollution", null, payloads, 5.0);
			CSPARQLAarhusParkingStream aps = new CSPARQLAarhusParkingStream("testuri", "streams/aarhus_parking.csv",
					ed, null, null);
			Thread th = new Thread(aps);
			th.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public CSPARQLAarhusParkingStream(String uri, String txtFile, EventDeclaration ed) throws IOException {
		super(uri);
		streamData = new CsvReader(String.valueOf(txtFile));
		this.ed = ed;
		streamData.setTrimWhitespace(false);
		streamData.setDelimiter(',');
		streamData.readHeaders();
	}

	public CSPARQLAarhusParkingStream(String uri, String txtFile, EventDeclaration ed, Date start, Date end)
			throws IOException {
		super(uri);
		logger.info("init");
		streamData = new CsvReader(String.valueOf(txtFile));
		this.ed = ed;
		streamData.setTrimWhitespace(false);
		streamData.setDelimiter(',');
		streamData.readHeaders();
		this.startDate = start;
		this.endDate = end;
	}

	@Override
	public void run() {
		logger.info("Starting sensor stream: " + this.getIRI());
		try {
			while (streamData.readRecord() && !stop) {
				Date obTime = sdf.parse(streamData.get("updatetime"));
				if (this.startDate != null && this.endDate != null) {
					if (obTime.before(this.startDate) || obTime.after(this.endDate)) {
						logger.debug(this.getIRI() + ": Disgarded observation @" + obTime);
						continue;
					}
				}
				// logger.debug("Reading data: " + streamData.toString());
				AarhusParkingObservation po = (AarhusParkingObservation) this.createObservation(streamData);
				// logger.debug("Reading data: " + new Gson().toJson(po));
				List<Statement> stmts = this.getStatements(po);
				for (Statement st : stmts) {
					try {
						logger.debug(this.getIRI() + " Streaming: " + st.toString());
						final RdfQuadruple q = new RdfQuadruple(st.getSubject().toString(), st.getPredicate()
								.toString(), st.getObject().toString(), System.currentTimeMillis());
						this.put(q);
						logger.info(this.getIRI() + " Streaming: " + q.toString());
						logger.debug("Messages streamed to CSPARQL engine successfully.");

					} catch (Exception e) {
						e.printStackTrace();
						logger.error(this.getIRI() + " CSPARQL streamming error.");
					}
					// messageByte += st.toString().getBytes().length;
				}
				logger.debug("Messages streamed to CSPARQL successfully.");
				try {
					if (this.getRate() != 1.0)
						Thread.sleep(sleep);
				} catch (Exception e) {

					e.printStackTrace();
					this.stop();
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Unexpected thread termination");

		} finally {
			logger.info("Stream Terminated: " + this.getIRI());
			this.stop();
		}

	}

	@Override
	protected List<Statement> getStatements(SensorObservation so) throws NumberFormatException, IOException {
		return DataWrapper.getAarhusParkingStatements(so, ed);
	}

	@Override
	protected SensorObservation createObservation(Object data) {
		AarhusParkingObservation apo = (AarhusParkingObservation) DataWrapper.getAarhusParkingObservation(
				(CsvReader) data, ed);
		DataWrapper.waitForInterval(currentObservation, apo, startDate, getRate());
		this.currentObservation = apo;
		return apo;

	}

}
