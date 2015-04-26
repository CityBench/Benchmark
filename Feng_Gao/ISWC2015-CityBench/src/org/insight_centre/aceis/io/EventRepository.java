package org.insight_centre.aceis.io;

import java.util.*;
import java.util.Map.Entry;

//import org.insight_centre.aceis.engine.ReusabilityHierarchy;
import org.insight_centre.aceis.eventmodel.*;
//import org.insight_centre.aceis.utils.test.Simulator2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import compositionAlgorithms.*;

/**
 * @author feng
 * 
 *         An repository for event service declarations, their event patterns and the hierarchy
 */
public class EventRepository {
	public static final Logger logger = LoggerFactory.getLogger(EventRepository.class);

	// private Map<String, ArrayList<String>> dependencies;
	private Map<String, EventDeclaration> eds;

	private Map<String, EventPattern> eps;

	// private ReusabilityHierarchy reusabilityHierarchy;

	public EventRepository() {
		super();
		this.eds = new HashMap<String, EventDeclaration>();
		this.eps = new HashMap<String, EventPattern>();
		// this.dependencies = new HashMap<String, ArrayList<String>>();// obsolete
		// this.reusabilityHierarchy = new ReusabilityHierarchy();
		// this.hierarchy = new HashMap<List<String>, List<String>>();
	}

	/**
	 * @throws Exception
	 * 
	 *             creates the reusablity hierarchy
	 */
	// public void buildHierarchy() throws Exception {
	// for (Entry e : this.eps.entrySet()) {
	// reusabilityHierarchy.getCandidates().add(((EventPattern) e.getValue()));
	// }
	// reusabilityHierarchy.buildHierarchy();
	// // this.hierarchy = reusabilityHierarchy.getHierarchy();
	//
	// }

	// private Map<List<String>, List<String>> hierarchy;

	public EventDeclaration getEDByEPId(String epid) {
		// System.out.println("Getting ed for: " + epid);
		for (Entry<String, EventDeclaration> entry : this.eds.entrySet()) {
			if (entry.getValue().getEp() != null)
				if (entry.getValue().getEp().getID().equals(epid))
					return entry.getValue();
		}
		return null;
	}

	public Map<String, EventDeclaration> getEds() {
		return eds;
	}

	public Map<String, EventPattern> getEps() {
		return eps;
	}

	// public Map<List<String>, List<String>> getHierarchy() throws Exception {
	// return this.reusabilityHierarchy.getPrimitiveHierarchy();
	//
	// }

	// public ReusabilityHierarchy getReusabilityHierarchy() {
	// return reusabilityHierarchy;
	// }

	// public void clearHierarchy() {
	// this.reusabilityHierarchy = new ReusabilityHierarchy();
	// }

	public void setEds(Map<String, EventDeclaration> eds) {
		this.eds = eds;
	}

	public void setEps(Map<String, EventPattern> eps) {
		this.eps = eps;
	}

	// public void setReusabilityHierarchy(ReusabilityHierarchy reusabilityHierarchy) {
	// this.reusabilityHierarchy = reusabilityHierarchy;
	// }
}
