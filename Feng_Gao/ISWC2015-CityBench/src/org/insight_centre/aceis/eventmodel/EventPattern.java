package org.insight_centre.aceis.eventmodel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

//import org.insight_centre.aceis.engine.Comparator;
import org.insight_centre.aceis.io.rdf.TextFileManager;
//import org.insight_centre.aceis.utils.test.Simulator2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author feng
 * 
 *         An event pattern is the data model describing the correlations of the member events of a complex event. It
 *         also provides operations related to canonical event pattern creation and event pattern modification etc.
 */
public class EventPattern implements Cloneable, Comparable<EventPattern> {

	private List<EventDeclaration> eds; // member event nodes
	private List<EventOperator> eos; // operator nodes
	private Map<String, List<Filter>> filters; // filters map : <nodeId, list of filters attached>
	private String ID;
	private int idCnt;
	private boolean isQuery = false;
	private Map<String, List<String>> provenanceMap;
	private List<Selection> selections;
	private Map<String, String> temporalMap;
	private int timeWindow;
	private int trafficDemand;
	private Logger logger = LoggerFactory.getLogger(this.getClass());

	public EventPattern() {
		this.ID = "";
		this.eds = new ArrayList<EventDeclaration>();
		this.eos = new ArrayList<EventOperator>();
		this.provenanceMap = new HashMap<String, List<String>>();
		this.temporalMap = new HashMap<String, String>();
		this.timeWindow = 0;
		this.trafficDemand = 0;
		this.idCnt = 0;
		this.filters = new HashMap<String, List<Filter>>();
		this.selections = new ArrayList<Selection>();
	}

	public EventPattern(String iD, List<EventDeclaration> eds, List<EventOperator> eos,
			Map<String, List<String>> provenanceMap, Map<String, String> temporalMap, int timeWindow, int trafficDemand) {
		super();
		ID = iD;
		this.eds = eds;
		this.eos = eos;
		this.provenanceMap = provenanceMap;
		this.temporalMap = temporalMap;
		this.timeWindow = timeWindow;
		this.trafficDemand = trafficDemand;
		// this.serviceMap = serviceMap;
		// this.
		// this.(this.());
		this.idCnt = eds.size() + eos.size();
		this.filters = new HashMap<String, List<Filter>>();
		this.selections = new ArrayList<Selection>();
	}

	public void addDSTs(List<EventPattern> dsts, EventOperator root) {

		for (int i = 0; i < dsts.size(); i++) {
			// System.out.println("Adding dst: " + dsts.get(i));
			EventPattern dst = dsts.get(i);
			this.getEds().addAll(dst.getEds());
			this.getEos().addAll(dst.getEos());
			this.getProvenanceMap().putAll(dst.getProvenanceMap());
			this.getTemporalMap().putAll(dst.getTemporalMap());
			this.getProvenanceMap().get(root.getID()).add(dst.getRootId());
			// System.out.println("Adding dst root: " + root.getID() + " sub root: " + dst.getRootId());
			if (root.getOpt() == OperatorType.seq || root.getOpt() == OperatorType.rep) {
				if (i < dsts.size() - 1) {
					this.temporalMap.put(dst.getRootId(), dsts.get(i + 1).getRootId());
				}
			}
			if (dst.getFilters() != null)
				this.filters.putAll(dst.getFilters());
			if (dst.getSelections() != null)
				this.selections.addAll(dst.getSelections());
		}
	}

	/**
	 * @return
	 * @throws CloneNotSupportedException
	 * @throws NodeRemovalException
	 * 
	 *             aggregates qos vector for the event pattern.
	 */
	// public QosVector aggregateQos() throws CloneNotSupportedException, NodeRemovalException {
	// if (this.getEos().size() == 0)
	// return this.getEds().get(0).getExternalQos();
	// // System.out.println("Aggregating: " + this.toString());
	// List<EventPattern> dsts = this.getDirectSubtrees();
	// String rootID = this.getRootId();
	// if (this.getNodeById(rootID) instanceof EventOperator) {
	// if (((EventOperator) this.getNodeById(rootID)).getOpt() == OperatorType.rep
	// || ((EventOperator) this.getNodeById(rootID)).getOpt() == OperatorType.seq)
	// Comparator.sortDSTs(dsts, this);
	// }
	//
	// List<QosVector> dstVectors = new ArrayList<QosVector>();
	// for (EventPattern ep : dsts)
	// dstVectors.add(ep.aggregateQos());
	//
	// // qos.setLatency(dstVectors.get(dstVectors.size()-1).getLatency());
	// int totalPrice = 0;
	// int totalSecurity = 5;
	// Double totalAvailability = 1.0;
	// for (QosVector v : dstVectors) {
	// totalPrice += v.getPrice();
	// if (totalSecurity >= v.getSecurity())
	// totalSecurity = v.getSecurity();
	// totalAvailability = totalAvailability * v.getAccuracy();
	// }
	// int totalLatency = 0;
	// Double totalReliability = 1.0;
	// EventOperator rootOp = (EventOperator) this.getNodeById(this.getRootId());
	// OperatorType rootOpt = rootOp.getOpt();
	// if (rootOpt == OperatorType.seq) {
	// totalLatency = dstVectors.get(dstVectors.size() - 1).getLatency();// last event service latency
	//
	// Double minFreq = 1000.0;
	// for (int i = 0; i < dsts.size(); i++) {
	// Double minDstFreq = dsts.get(i).getFrequency() * dstVectors.get(i).getReliability();
	// if (minFreq >= minDstFreq)
	// minFreq = minDstFreq;
	// }
	// totalReliability = minFreq / this.getFrequency();
	// } else if (rootOpt == OperatorType.rep) {
	// totalLatency = dstVectors.get(dstVectors.size() - 1).getLatency();// last event service latency
	//
	// Double minFreq = 1000.0;
	// for (int i = 0; i < dsts.size(); i++) {
	// Double minDstFreq = dsts.get(i).getFrequency() * dstVectors.get(i).getReliability();
	// if (minFreq >= minDstFreq)
	// minFreq = minDstFreq;
	// }
	// totalReliability = minFreq / (this.getFrequency() * rootOp.getCardinality());
	// } else if (rootOpt == OperatorType.and) {
	// int latencySum = 0;
	// for (QosVector v : dstVectors) {
	// latencySum += v.getLatency();
	// }
	// totalLatency = latencySum / dstVectors.size();// average event service latency
	//
	// Double minFreq = 1000.0;
	// for (int i = 0; i < dsts.size(); i++) {
	// Double minDstFreq = (dsts.get(i).getFrequency()) * (dstVectors.get(i).getReliability());
	// if (minFreq >= minDstFreq)
	// minFreq = minDstFreq;
	// }
	// totalReliability = minFreq / this.getFrequency();
	//
	// } else {
	// int latencySum = 0;
	// for (QosVector v : dstVectors) {
	// latencySum += v.getLatency();
	// }
	// totalLatency = latencySum / dstVectors.size();// average event service latency
	//
	// Double maxFreq = 0.0;
	// for (int i = 0; i < dsts.size(); i++) {
	// Double minDstFreq = dsts.get(i).getFrequency() * dstVectors.get(i).getReliability();
	// if (maxFreq <= minDstFreq)
	// maxFreq = minDstFreq;
	// }
	// totalReliability = maxFreq / this.getFrequency();
	// }
	// QosVector qos = new QosVector(totalLatency, totalPrice, totalSecurity, totalAvailability, totalReliability,
	// this.getTrafficDemand());
	// // qos.setTraffic(this.getTrafficDemand());
	// return qos;
	// }

	public void appendDST(EventDeclaration ed) throws NodeRemovalException {

		this.getEds().add(ed);
		this.getProvenanceMap().get(this.getRootId()).add(ed.getnodeId());
		EventOperator root = (EventOperator) this.getNodeById(this.getRootId());
		if (root.getOpt() == OperatorType.seq) {
			List<String> childIds = this.getChildIds(root.getID());
			this.sortSequenceChilds(childIds);
			this.getTemporalMap().put(childIds.get(childIds.size() - 1), ed.getnodeId());
		}

	}

	private void appendEventPatterns(List<EventDeclaration> eds) {
		for (EventDeclaration ed : eds) {
			// System.out.println("Appending: " + ed.getEp());
			this.eds.addAll(ed.getEp().getEds());
			this.eos.addAll(ed.getEp().getEos());
			this.provenanceMap.putAll(ed.getEp().getProvenanceMap());
			this.temporalMap.putAll(ed.getEp().getTemporalMap());
			String parentid = this.getParent(ed.getnodeId());
			if (parentid != null) {
				EventOperator parent = (EventOperator) this.getNodeById(parentid);
				this.getProvenanceMap().get(parentid).remove(ed.getnodeId());
				this.getProvenanceMap().get(parentid).add(ed.getEp().getRootId());
				if (parent.getOpt() == OperatorType.seq || parent.getOpt() == OperatorType.rep) {
					String successor = this.getSuccessor(ed.getnodeId());
					String precessor = this.getPredecessor(ed.getnodeId());
					if (successor != null) {
						this.temporalMap.remove(ed.getnodeId());
						this.temporalMap.put(ed.getEp().getRootId(), successor);
					}
					if (precessor != null) {
						this.temporalMap.remove(precessor);
						this.temporalMap.put(precessor, ed.getEp().getRootId());
					}
				}
			}
			this.getEds().remove(ed);
		}

	}

	/**
	 * @param i
	 *            apply offset to the node ids, invoked only during query pattern initialization
	 */
	public void applyExternalOffset(int i) {

	}

	/**
	 * @param offset
	 * 
	 *            apply the specified offset for all node ids, this method is invoked when replication of subtrees are
	 *            needed
	 */
	private void applyOffset(int offset) {
		// change node ids
		Map<String, String> edMap = new HashMap<String, String>();
		Map<String, String> eoMap = new HashMap<String, String>();
		for (EventDeclaration ed : this.getEds()) {
			String originalId = ed.getnodeId();
			ed.setnodeId(offset + "");
			edMap.put(originalId, offset + "");
			offset += 1;
		}
		for (EventOperator eo : this.getEos()) {
			String originalId = eo.getID();
			eo.setID(offset + "");
			eoMap.put(originalId, offset + "");
			offset += 1;
		}
		// modify temporal maps
		Map<String, String> newTemporalMap = new HashMap<String, String>();
		Iterator<Entry<String, String>> tempIt = this.getTemporalMap().entrySet().iterator();
		while (tempIt.hasNext()) {
			Entry<String, String> e = tempIt.next();
			String key = e.getKey();
			String newKey = "";
			if (edMap.containsKey(key))
				newKey = edMap.get(key);
			else
				newKey = eoMap.get(key);
			String value = e.getValue();
			String newValue = "";
			if (edMap.containsKey(value))
				newValue = edMap.get(value);
			else
				newValue = eoMap.get(value);
			newTemporalMap.put(newKey, newValue);
		}
		this.setTemporalMap(newTemporalMap);

		// change provenance map ids
		Iterator<Entry<String, List<String>>> proIt = this.getProvenanceMap().entrySet().iterator();
		Map<String, List<String>> newProvenanceMap = new HashMap<String, List<String>>();
		while (proIt.hasNext()) {
			Entry<String, List<String>> e = proIt.next();
			String key = e.getKey();
			String newkey = "";
			if (edMap.containsKey(key))
				newkey = edMap.get(key);
			else
				newkey = eoMap.get(key);

			List<String> values = e.getValue();

			List<String> newValues = new ArrayList<String>();
			for (String value : values) {
				String newValue = "";
				if (edMap.containsKey(value))
					newValue = edMap.get(value);
				else
					newValue = eoMap.get(value);
				newValues.add(newValue);
			}
			newProvenanceMap.put(newkey, newValues);
		}
		this.setProvenanceMap(newProvenanceMap);

	}

	// private List<Filter> filters;
	public EventPattern clone() throws CloneNotSupportedException {

		EventPattern clone = new EventPattern(this.getID() + "clone", new ArrayList<EventDeclaration>(),
				new ArrayList<EventOperator>(), new HashMap<String, List<String>>(), new HashMap<String, String>(),
				this.timeWindow, this.trafficDemand);
		List<EventDeclaration> edsclone = new ArrayList<EventDeclaration>();
		for (EventDeclaration ed : this.getEds()) {
			EventDeclaration edclone = ed.clone();
			edsclone.add(edclone);
		}
		clone.setEds(edsclone);
		if (this.getSelections() != null) {
			List<Selection> selClones = new ArrayList<Selection>();
			clone.setSelections(selClones);
			for (Selection sel : this.selections)
				selClones.add(sel.clone());
		}
		List<EventOperator> eosClone = new ArrayList<EventOperator>();
		for (EventOperator eo : this.getEos()) {
			EventOperator eoClone = new EventOperator(eo.getOpt(), eo.getCardinality(), eo.getID());
			eosClone.add(eoClone);
		}
		clone.setEos(eosClone);
		for (Map.Entry en : this.getProvenanceMap().entrySet()) {
			clone.getProvenanceMap().put((String) en.getKey(), new ArrayList<String>());
			for (String s : (List<String>) en.getValue()) {
				clone.getProvenanceMap().get((String) en.getKey()).add(s);
			}
		}
		for (Map.Entry en : this.getTemporalMap().entrySet()) {
			clone.getTemporalMap().put((String) en.getKey(), (String) en.getValue());
		}
		// for (Map.Entry en : this.getServiceMap().entrySet()) {
		// clone.getServiceMap().put((String) en.getKey(), (EventDeclaration) en.getValue());
		// }

		clone.setIdCnt(this.idCnt);
		return clone;
	}

	/**
	 * @param repOrSeqId
	 * @throws Exception
	 */
	private void expandRepetition(String repOrSeqId) throws Exception {
		// System.out.println("Expanding: " + (repOrSeqId));
		// System.out.println("Expanding before: " + this.provenanceMap + "\n" + this.temporalMap);
		while (hasRepChild(repOrSeqId)) {
			List<String> childIds = this.getChildIds(repOrSeqId);
			for (String s : childIds) {
				Object childNode = this.getNodeById(s);
				if (childNode instanceof EventOperator) {
					EventOperator eo = (EventOperator) childNode;
					if (eo.getOpt() == OperatorType.rep) {
						int cardinality = eo.getCardinality();
						this.sequenceReplicate(s, cardinality);
					}
				}
			}
		}
		// System.out.println("Expanding after: " + this.provenanceMap + "\n" + this.temporalMap);
	}

	public List<String> getAncestors(String nodeId) {
		String currentNode = nodeId;
		ArrayList<String> results = new ArrayList<String>();
		while (this.getParent(currentNode) != null) {
			String parent = this.getParent(currentNode);
			results.add(parent);
			currentNode = parent;
		}
		return results;
	}

	// public EventPattern getCanonicalPattern() throws Exception {
	// // TODO sort out filters
	// EventPattern completeEP = this.getCompletePattern();
	// EventPattern canonicalEP = completeEP.getReducedPattern();
	// this.distributeSelections();
	// return canonicalEP;
	// }

	private void distributeSelections() {
		for (Selection sel : this.getSelections()) {
			Object node = this.getNodeById(sel.getProvidedBy());
			if (node instanceof EventOperator || node == null) {
				for (EventDeclaration ed : this.getEds()) {
					List<String> payloads = ed.getPayloads();
					boolean matched = false;
					for (String p : payloads) {
						if (p.contains(sel.getPropertyName())) {
							sel.setProvidedBy(ed.getnodeId());
							matched = true;
							break;
						}
					}
					if (matched)
						break;
				}
			}
		}

	}

	public List<String> getChildIds(String nodeId) {
		ArrayList<String> results = new ArrayList<String>();
		if (this.getProvenanceMap().containsKey(nodeId))
			results.addAll(this.getProvenanceMap().get(nodeId));
		return results;
	}

	public EventPattern getCompletePattern() {
		// TODO need to consider for forwarding events which subscribes and re-publishes the events from other sources
		while (this.getComplexED().size() != 0) {
			this.appendEventPatterns(this.getComplexED());
		}
		// System.out.println("Complete ep: " + this.toString());
		return this;
	}

	private List<EventDeclaration> getComplexED() {
		List<EventDeclaration> eds = new ArrayList<EventDeclaration>();
		for (EventDeclaration ed : this.getEds()) {
			if (ed.getEventType().equals("complex") || ed.getEventType().contains("EP-") || ed.getEp() != null)
				eds.add(ed);
		}
		return eds;
	}

	public List<String> getDesendants(String nodeId) {
		ArrayList<String> results = new ArrayList<String>();
		ArrayList<String> nodesToCheck = new ArrayList<String>();
		nodesToCheck.add(nodeId);
		while (nodesToCheck.size() > 0) {
			List<String> newChilds = new ArrayList<String>();
			for (String s : nodesToCheck) {
				if (this.getProvenanceMap().get(s) != null) {
					results.addAll(this.getProvenanceMap().get(s));
					newChilds.addAll(this.getProvenanceMap().get(s));
				}
			}
			nodesToCheck = new ArrayList<String>();
			nodesToCheck.addAll(newChilds);
		}
		return results;
	}

	public List<EventPattern> getDirectSubtrees() throws CloneNotSupportedException {
		List<String> subRoots = this.getProvenanceMap().get(this.getRootId());
		ArrayList<EventPattern> results = new ArrayList<EventPattern>();
		if (subRoots == null)
			System.out.println("Null subroots: " + this.toString());
		for (String s : subRoots) {
			results.add(this.getSubtreeByRoot(s));
		}
		return results;
	}

	public List<EventDeclaration> getEds() {
		return eds;
	}

	public List<EventOperator> getEos() {
		return eos;
	}

	public Map<String, List<Filter>> getFilters() {
		return filters;
	}

	public Double getFrequency() {
		Double result = this.getFrequencyForNode(this.getRootId());
		return result;
	}

	private Double getFrequencyForNode(String rootId) {
		if (this.getNodeById(rootId) instanceof EventDeclaration)
			return ((EventDeclaration) this.getNodeById(rootId)).getFrequency();
		else {
			EventOperator rootOp = (EventOperator) this.getNodeById(rootId);
			List<Double> childFrequencies = new ArrayList<Double>();
			List<String> childIds = this.getChildIds(rootId);
			for (String s : childIds) {
				childFrequencies.add(this.getFrequencyForNode(s));
			}
			Double freq = -1.0;
			if (rootOp.getOpt() == OperatorType.seq || rootOp.getOpt() == OperatorType.and) {
				Double minFreq = -1.0;
				for (Double d : childFrequencies) {
					if (minFreq < 0)
						minFreq = d;
					else if (d < minFreq)
						minFreq = d;
				}
				freq = minFreq;
			} else if (rootOp.getOpt() == OperatorType.rep) {
				Double minFreq = -1.0;
				for (Double d : childFrequencies) {
					if (minFreq < 0)
						minFreq = d;
					else if (d < minFreq)
						minFreq = d;
				}
				minFreq = minFreq / (rootOp.getCardinality() + 0.0);
				freq = minFreq;
			} else if (rootOp.getOpt() == OperatorType.or) {
				Double sumFreq = 0.0;
				for (Double d : childFrequencies)
					sumFreq += d;
				freq = sumFreq;
			}
			return freq;
		}
	}

	public int getHeight() {
		int maxDepth = 0;
		List<String> nodes = this.getDesendants(this.getRootId());
		for (String s : nodes) {
			int depth = this.getAncestors(s).size();
			if (depth > maxDepth)
				maxDepth = depth;
		}
		return maxDepth;
	}

	public String getID() {
		return ID;
	}

	public int getIdCnt() {
		return this.idCnt;
	}

	public Object getNodeById(String id) {
		for (EventDeclaration ed : this.eds)
			if (ed.getnodeId().equals(id))
				return ed;
		for (EventOperator eo : this.eos)
			if (eo.getID().equals(id))
				return eo;
		return null;
	}

	private Integer getNodeDepth(String nodeID) {
		List<String> ancestors = this.getAncestors(nodeID);
		return ancestors.size();
	}

	private Integer getNodeHeight(String nodeId) {
		// System.out.println("getting node height: " + nodeId + " in ep: " + this.getID());
		Object node = this.getNodeById(nodeId);
		if (node == null) {
			// System.out.println("null node for: " + nodeId + " in ep: " + this.toString());
		}
		if (node instanceof EventDeclaration) {
			// System.out.println(nodeId + " is ED.");
			return 0;
		} else {
			// System.out.println(nodeId + " is NOT ED. " + node.getClass());
		}
		EventOperator eo = (EventOperator) node;
		int depth = this.getAncestors(eo.getID()).size();
		int maxChildDepth = 0;
		List<String> desendants = this.getDesendants(eo.getID());
		for (String s : desendants) {
			if (this.getAncestors(s).size() > maxChildDepth)
				maxChildDepth = this.getAncestors(s).size();
		}
		return maxChildDepth - depth;
	}

	private List<String> getNodeIdsByDepth(int depth) {
		List<String> results = new ArrayList<String>();
		if (depth == 0)
			results.add(this.getRootId());
		else {
			for (String s : this.getDesendants(this.getRootId())) {
				if (this.getNodeDepth(s) == depth)
					results.add(s);
			}
		}
		return results;
	}

	private List<String> getNodeIdsByHeight(int height) {
		List<String> results = new ArrayList<String>();
		for (EventOperator eo : this.getEos()) {
			int depth = this.getAncestors(eo.getID()).size();
			int maxChildDepth = 0;
			List<String> desendants = this.getDesendants(eo.getID());
			for (String s : desendants) {
				if (this.getAncestors(s).size() > maxChildDepth)
					maxChildDepth = this.getAncestors(s).size();
			}
			if ((maxChildDepth - depth) == height)
				results.add(eo.getID());
		}
		return results;
	}

	private String getNodeStr(String rootId) throws NodeRemovalException {
		if (rootId == null)
			return "";
		// System.out.println("Presenting node: " + rootId);
		// System.exit(0);
		// }
		// System.out.println("getting nodestr: " + rootId);
		String str = this.getNodeById(rootId).toString();
		List<String> childIds = this.getChildIds(rootId);
		if (childIds.size() > 0) {
			str = str + "(";
			if (this.getNodeById(rootId) instanceof EventOperator)
				if (((EventOperator) this.getNodeById(rootId)).getOpt() == OperatorType.rep
						|| ((EventOperator) this.getNodeById(rootId)).getOpt() == OperatorType.seq)
					childIds = this.sortSequenceChilds(childIds);
		}
		for (int i = 0; i < childIds.size(); i++) {
			str = str + this.getNodeStr(childIds.get(i)) + ",";
		}
		if (childIds.size() > 0) {
			str = str.substring(0, str.length() - 1);
			str = str + ")";
		}
		return str;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public String getParent(String nodeId) {
		Iterator it = this.getProvenanceMap().entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, List<String>> e = (Entry<String, List<String>>) it.next();
			if (e.getValue().contains(nodeId))
				return e.getKey();
		}
		// System.out.println("no parent for: " + nodeId);
		return null;
	}

	public String getPredecessor(String nodeId) {
		for (Entry<String, String> entry : this.getTemporalMap().entrySet()) {
			if (entry.getValue().equals(nodeId))
				return entry.getKey();
		}

		return null;
	}

	public Map<String, List<String>> getProvenanceMap() {
		return provenanceMap;
	}

	// public EventPattern getReducedPattern() throws Exception {
	// // System.out.println("==============================");
	// // System.out.println("Reducing: " + this.toString());
	//
	// this.liftTree(this.getRootId());
	// int height = this.getHeight();
	// if (height < 1)
	// return this;
	// List<String> nodesToMerge = new ArrayList<String>();
	//
	// for (int i = height - 1; i >= 0; i--) {
	// nodesToMerge = this.getNodeIdsByDepth(i);
	// for (String s : nodesToMerge) {
	// if (this.merge(s))
	// this.liftTree(s);
	// }
	// }
	// // this.updateIdCnt();
	//
	// // System.out.println("Reduction complete!");
	// // System.out.println("Reduced: " + this.toString());
	// return this;
	// }

	/**
	 * @param subSequence
	 * @param totalSequence
	 * @param start
	 * @param end
	 * @return a string in the form of "a-b" indicating the start index of the repeating sequence with "a" and the end
	 *         index with "b"; if no repeating sequence is found, null is returned
	 * @throws CloneNotSupportedException
	 * @throws CatastrophicRemovalException
	 */
	// private String getRepeatingIndex(List<String> subSequence, List<String> totalSequence, int start, int end)
	// throws CloneNotSupportedException, NodeRemovalException {
	// // System.out.println("gettingRepeatIndex:" + subSequence + " " + totalSequence + " " + start + " " + end);
	// int length = subSequence.size();
	// List<ArrayList<String>> possibleRepeats = new ArrayList<ArrayList<String>>();
	// int numberOfLists = (totalSequence.size() - end - 1) / length;
	// for (int i = 0; i < numberOfLists; i++) {
	// ArrayList<String> list = new ArrayList<String>();
	// possibleRepeats.add(list);
	// }
	//
	// for (int i = end + 1; i < end + 1 + (numberOfLists * length); i++) {
	// int index1 = (i - end - 1) / length;
	//
	// String curId = totalSequence.get(i);
	// possibleRepeats.get(index1).add(curId);
	// }
	// int repeatingEnds = -1;
	// int matchCnt = 0;
	// for (int i = 0; i < possibleRepeats.size(); i++) {
	// boolean matched = true;
	// ArrayList<String> tempList = possibleRepeats.get(i);
	// for (int j = 0; j < tempList.size(); j++) {
	// if (!this.isIsomorhpic(subSequence.get(j), tempList.get(j))) {
	//
	// matched = false;
	// break;
	// } else {
	// repeatingEnds = totalSequence.indexOf(tempList.get(j));
	// matchCnt += 1;
	// }
	// }
	// if (!matched)
	// break;
	// }
	// if (matchCnt < length)
	// return null;
	// else {
	// String result = start + "-" + (repeatingEnds - (repeatingEnds - end) % length);
	// // System.out.println("repeating index result: " + result);
	// return result;
	// }
	// }

	public List<String> getOperatorIds() {
		List<String> results = new ArrayList<String>();
		for (EventOperator eo : this.getEos()) {
			// if (this.getParent(eo.getID()) == null)
			results.add(eo.getID());
		}
		return results;
	}

	public String getRootId() {
		for (EventOperator eo : this.getEos()) {
			if (this.getParent(eo.getID()) == null)
				return eo.getID();
		}
		for (EventDeclaration ed : this.getEds()) {
			if (this.getParent(ed.getnodeId()) == null)
				return ed.getnodeId();
		}
		logger.error("root id is null: " + this.getID());
		return null;
	}

	public List<Selection> getSelectionOnNode(String nodeId) {
		ArrayList<Selection> results = new ArrayList<Selection>();
		if (this.getNodeById(nodeId) instanceof EventDeclaration) {
			for (Selection sel : this.getSelections())
				if (sel.getProvidedBy().equals(nodeId))
					results.add(sel);
		} else {
			for (String desendant : this.getDesendants(nodeId)) {
				if (this.getNodeById(desendant) instanceof EventDeclaration)
					results.addAll(this.getSelectionOnNode(desendant));
			}
		}

		return results;
	}

	public List<Selection> getSelections() {
		return selections;
	}

	public Integer getSize() {
		return this.getEds().size() + this.getEos().size();
	}

	public EventPattern getSubtreeByRoot(String rootId) throws CloneNotSupportedException {
		// System.out.println("Getting subtree: " + rootId);
		// EventPattern clone = (EventPattern) this.clone();
		EventPattern result = new EventPattern(this.getID() + "-sub", new ArrayList<EventDeclaration>(),
				new ArrayList<EventOperator>(), new HashMap<String, List<String>>(), new HashMap<String, String>(),
				this.timeWindow, -1);
		ArrayList<String> nodesToAdd = new ArrayList<String>();
		HashMap<String, List<Filter>> newFilters = new HashMap<String, List<Filter>>();

		nodesToAdd.add(rootId);
		while (nodesToAdd.size() > 0) {
			List<String> newOps = new ArrayList<String>();

			for (String s : nodesToAdd) {
				Object node = this.getNodeById(s);
				if (node instanceof EventDeclaration) {
					EventDeclaration ed = ((EventDeclaration) node).clone();
					result.getEds().add(ed);// add event declaration
					if (this.getFilters() != null && this.getFilters().containsKey(ed.getnodeId()))
						newFilters.put(ed.getnodeId(), this.getFilters().get(ed.getnodeId())); // add filters
					// if (this.getSelectionOnNode(ed.getnodeId()) != null)
					// ;
				} else if (node instanceof EventOperator) {
					EventOperator eo = ((EventOperator) node).clone();
					result.getEos().add(eo);// add event operator
					newOps.add(s);
					if (this.getFilters() != null && this.getFilters().containsKey(eo.getID()))
						newFilters.put(eo.getID(), this.getFilters().get(eo.getID())); // add filters
				}
				if (!s.equals(rootId) && this.getTemporalMap().get(s) != null) {
					result.getTemporalMap().put(s, this.getTemporalMap().get(s));
				}

			}
			nodesToAdd = new ArrayList<String>();
			for (String s : newOps) {
				if (this.getProvenanceMap().get(s) != null) {
					result.getProvenanceMap().put(s, this.getProvenanceMap().get(s));
					nodesToAdd.addAll(this.getProvenanceMap().get(s));
				}
			}
		}
		if (newFilters.size() > 0)
			result.setFilters(newFilters);
		// add selections
		List<Selection> newSelections = new ArrayList<Selection>();
		for (EventDeclaration ed : result.eds) {
			List<Selection> selOnNode = this.getSelectionOnNode(ed.getnodeId());
			for (Selection sel : selOnNode) {
				newSelections.add(sel.clone());
			}
		}
		result.setSelections(newSelections);
		return result;
	}

	public List<EventPattern> getSubtreesByHeight(int height) throws CloneNotSupportedException {
		List<String> roots = this.getNodeIdsByHeight(height);
		List<EventPattern> results = new ArrayList<EventPattern>();
		for (String s : roots) {
			results.add(this.getSubtreeByRoot(s));
		}
		return results;
	}

	public String getSuccessor(String nodeId) {

		return this.getTemporalMap().get(nodeId);
	}

	public Map<String, String> getTemporalMap() {
		return temporalMap;
	}

	public int getTimeWindow() {
		return timeWindow;
	}

	public Double getTrafficDemand() {
		Double freqSum = 0.0;
		for (EventDeclaration ed : this.getEds())
			freqSum += ed.getFrequency();
		return freqSum;
	}

	public boolean hasFilterOn(String nodeId) {
		if (this.filters != null)
			if (this.filters.get(nodeId) != null)
				return true;
		return false;
	}

	private boolean hasRepChild(String repOrSeqId) {
		List<String> childIds = this.getChildIds(repOrSeqId);
		for (String s : childIds) {
			Object childNode = this.getNodeById(s);
			if (childNode instanceof EventOperator) {
				EventOperator eo = (EventOperator) childNode;
				if (eo.getOpt() == OperatorType.rep)
					return true;
			}
		}
		return false;
	}

	/**
	 * @param eo
	 * @param childIds
	 *            inserts an event operator for a set of nodes, updates the idcnt when done
	 */
	public void insertParent(EventOperator eo, List<String> childIds) {
		this.idCnt += 1;
		// System.out.println("inserting " + eo.toString() + "\n To: " + this.toString());
		this.getEos().add(eo);
		String parentId = this.getParent(childIds.get(0));

		if (parentId != null) {
			this.getProvenanceMap().get(parentId).removeAll(childIds);
			this.getProvenanceMap().get(parentId).add(eo.getID());
		}

		this.getProvenanceMap().put(eo.getID(), childIds);
		Iterator<Entry<String, String>> it = this.getTemporalMap().entrySet().iterator();
		String head = "";
		String tail = "";
		String nodeToRemove = "";
		while (it.hasNext()) {
			Entry<String, String> e = it.next();
			if (e.getKey().equals(childIds.get(childIds.size() - 1))) {
				tail = e.getValue();
				nodeToRemove = e.getKey();
			}
			if (e.getValue().equals(childIds.get(0)))
				head = e.getKey();

		}
		if (!nodeToRemove.equals(""))
			this.getTemporalMap().remove(nodeToRemove);
		if (!head.equals("")) {
			this.getTemporalMap().put(head, eo.getID());
		}
		if (!tail.equals("")) {
			this.getTemporalMap().put(eo.getID(), tail);
		}

		// eo.print();
		// System.out.println("after insert: " + this.toString());
		// this.print(0);
	}

	// private boolean isIsomorhpic(String node1, String node2) throws CloneNotSupportedException, NodeRemovalException
	// {
	// // System.out.println("Comparing: " + this.getNodeStr(node1));
	// // System.out.println("With: " + this.getNodeStr(node2));
	// return Comparator.isSubstitute(this.getSubtreeByRoot(node1), this.getSubtreeByRoot(node2));
	// }

	public boolean isQuery() {
		return isQuery;
	}

	/**
	 * @param rootId
	 * @return whether removed redundant operators
	 * @throws Exception
	 */
	public boolean lift(String rootId) throws Exception {
		// System.out.println("Lifting: " + rootId);
		Object node = this.getNodeById(rootId);
		Boolean modified = false;
		if (node instanceof EventOperator) {
			EventOperator rootOp = (EventOperator) node;
			List<String> childNodes = this.getChildIds(rootId);
			OperatorType rootOpt = rootOp.getOpt();
			if (rootOpt == OperatorType.seq) {
				if (childNodes.size() == 1) {
					this.removeNode(rootId);
					modified = true;
				} else {
					for (String childId : childNodes) {
						Object childNode = this.getNodeById(childId);
						if (childNode instanceof EventOperator) {
							EventOperator childOp = (EventOperator) childNode;
							if (childOp.getOpt() == OperatorType.seq) {
								this.removeNode(childId);
								modified = true;
							}
						}
					}
				}
			} else if (rootOpt == OperatorType.rep) {
				if (childNodes.size() == 1) {
					if (this.getNodeById(childNodes.get(0)) instanceof EventOperator) {
						EventOperator childOp = (EventOperator) this.getNodeById(childNodes.get(0));
						if (childOp.getOpt() == OperatorType.seq || childOp.getOpt() == OperatorType.rep) {
							rootOp.setCardinality(rootOp.getCardinality() * childOp.getCardinality());
							// rootOp.setID(rootOp.getID().split("-")[0].substring(0, 7) + rootOp.getCardinality() + "-"
							// + rootOp.getID().split("-")[1]);
							this.removeNode(childNodes.get(0));
							modified = true;
						}
					}
				} else {
					for (String childId : childNodes) {
						Object childNode = this.getNodeById(childId);
						if (childNode instanceof EventOperator) {
							EventOperator childOp = (EventOperator) childNode;
							if (childOp.getOpt() == OperatorType.seq) {
								this.removeNode(childId);
								modified = true;
							}
						}
					}
				}
			} else {
				if (childNodes.size() == 1) {
					// System.out.println("Removing root during lift: " + rootId);
					this.removeNode(rootId);
					modified = true;
				} else {
					for (String childId : childNodes) {
						Object childNode = this.getNodeById(childId);
						if (childNode instanceof EventOperator) {
							EventOperator childOp = (EventOperator) childNode;
							if (childOp.getOpt() == rootOpt) {
								this.removeNode(childId);
								modified = true;
							}
						}
					}
				}
			}
		}
		// System.out.println("Lifted: " + modified);
		return modified;
	}

	/**
	 * @param rootId
	 * 
	 * @throws Exception
	 * 
	 *             invokes lift method for each node in a tree
	 */
	public void liftTree(String rootId) throws Exception {
		// System.out.println("LiftingTree: " + rootId);
		// System.out.println("LiftingTree Before: " + this.provenanceMap + "\n" + this.temporalMap);
		List<String> nodesToLift = new ArrayList<String>();
		nodesToLift.add(rootId);
		boolean liftedAny = false;
		while (nodesToLift.size() > 0) {
			List<String> nextNodesToLift = new ArrayList<String>();
			for (String s : nodesToLift) {
				if (this.getNodeHeight(s) > 1) {
					boolean lifted = this.lift(s);
					while (lifted == true) {
						liftedAny = true;
						// if (this.getNodeHeight(s) > 1)
						lifted = this.lift(s);
						// else {

						// }
					}
				}
				List<String> childNodes = this.getChildIds(s);
				if (childNodes.size() > 0)
					nextNodesToLift.addAll(childNodes);
			}
			nodesToLift = new ArrayList<String>();
			nodesToLift.addAll(nextNodesToLift);
		}
		// if (liftedAny)
		// System.out.println("LiftingTree After: " + this.provenanceMap + "\n" + this.temporalMap);
		// else
		// System.out.println("LiftingTree After: no change.");
	}

	/**
	 * @param rootId
	 * @return
	 * @throws Exception
	 * 
	 *             merges direct sub trees for a node
	 */
	// public boolean merge(String rootId) throws Exception {
	// // System.out.println("Merging: " + rootId);
	// // System.out.println("In: " + this.toString());
	// Boolean modified = false;
	// Object rootNode = this.getNodeById(rootId);
	// if (rootNode instanceof EventDeclaration)
	// return false;
	// EventOperator rootOp = (EventOperator) rootNode;
	// OperatorType rootOpt = rootOp.getOpt();
	// List<String> childNodes = this.getChildIds(rootId);
	// if (rootOpt == OperatorType.seq || rootOpt == OperatorType.rep) {
	// EventPattern original = this.getSubtreeByRoot(rootId).clone();
	// Map<String, List<String>> originalMap = original.getProvenanceMap();
	// // System.out.println("Original: " + original.toString());
	// // System.out.println("Original Temp: " + original.getTemporalMap());
	// this.expandRepetition(rootId);
	// List<String> sortedChildIds = new ArrayList<String>();
	// // System.out.println("Sorting from merge:");
	// sortedChildIds = this.sortSequenceChilds(this.getChildIds(rootId));
	// this.sequenceFold(sortedChildIds);
	// EventPattern folded = this.getSubtreeByRoot(rootId);
	// Map<String, List<String>> foldedMap = folded.getProvenanceMap();
	// // System.out.println("Folded: " + folded.toString());
	// // System.out.println("Folded Temp: " + folded.getTemporalMap());
	// // System.out.println("Original: " + original.toString());
	// if (originalMap != foldedMap)
	// modified = true;
	// // May require testing on modification detection
	// } else {
	// List<String> nodesToRemove = new ArrayList<String>();
	// for (String childId : childNodes) {
	// if (nodesToRemove.contains(childId))
	// continue;
	// for (String childId2 : childNodes) {
	// if (childId.equals(childId2))
	// continue;
	// if (Comparator.isCanonicalSubstitute(this.getSubtreeByRoot(childId),
	// this.getSubtreeByRoot(childId2))) {
	// nodesToRemove.add(childId2);
	// modified = true;
	// }
	// }
	// }
	// for (String nodeToRemove : nodesToRemove)
	// this.removeSubTreeById(nodeToRemove, true);
	//
	// }
	// // if (modified)
	// // System.out.println("Merging after: " + this.toString());
	// // else
	// // System.out.println("Merging after: no change.");
	// return modified;
	// }

	public void print(int i) {
		if (i == 0) {// shallow print
			System.out.println(this.getID() + ": ");
			System.out.println("provenance: " + this.getProvenanceMap());
			System.out.println("sequence: " + this.getTemporalMap());
		} else {// deep print

		}
	}

	/**
	 * @param nodeId
	 *            when a nodes is removed, all edges linking to the node are also removed, in the provenance map, its
	 *            parent will link to all its child nodes; in the temporal map, its predecessor will link to its
	 *            successor.
	 * @throws Exception
	 */
	private void removeNode(String nodeId) throws Exception {
		// Simulator2.logger.write("removing: " + nodeId);
		Object node = this.getNodeById(nodeId);
		// List<Selection> selsToRemove = this.getSelectionOnNode(nodeId);

		String parentId = "";
		if (this.getParent(nodeId) != null)
			parentId = this.getParent(nodeId);
		String sucessor = "";
		if (this.getTemporalMap().containsKey(nodeId))
			sucessor = this.getTemporalMap().get(nodeId);
		String precessor = "";
		if (this.getTemporalMap().containsValue(nodeId)) {
			Iterator<Entry<String, String>> it = this.getTemporalMap().entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, String> e = (Entry<String, String>) it.next();
				if (e.getValue().equals(nodeId)) {
					precessor = e.getKey();
					break;
				}
			}
		}

		List<String> childIds = this.getChildIds(nodeId);

		// drop provenance relation parent - node
		if (!parentId.equals(""))
			this.getProvenanceMap().get(parentId).remove(nodeId);
		// drop temporal relation precessor - node
		if (!precessor.equals(""))
			this.getTemporalMap().remove(precessor);
		// drop temporal relation node - sucessor
		if (!sucessor.equals(""))
			this.getTemporalMap().remove(nodeId);

		if (childIds.size() == 0) {// node is a leave
			// create temporal relation precessor - sucessor
			if (!precessor.equals("") && !sucessor.equals(""))
				this.getTemporalMap().put(precessor, sucessor);

		} else {// node is an operator
			// drop provenace relation node - childids
			this.getProvenanceMap().remove(nodeId);
			// create provenance relation parent - childIds
			if (!parentId.equals(""))
				this.getProvenanceMap().get(parentId).addAll(childIds);
			// locate the head and tail among child nodes
			String childHead = "";
			String childTail = "";
			if (childIds.size() == 1) {
				childHead = childIds.get(0);
				childTail = childHead;
			} else if (childIds.size() > 1) {
				EventOperator eo = (EventOperator) node;
				if (eo.getOpt() == OperatorType.seq || eo.getOpt() == OperatorType.rep) {
					for (String s : childIds) {
						if (!this.getTemporalMap().containsKey(s))
							childTail = s;
						else if (!this.getTemporalMap().containsValue(s))
							childHead = s;
					}
				} else if (!precessor.equals("") && !sucessor.equals(""))
					this.getTemporalMap().put(precessor, sucessor);
				// throw new CatastrophicRemovalException(nodeId);
			}
			// if (childHead.equals("") || childTail.equals(""))
			// throw new Exception("Cannot find child head and tail.");
			// create temporal relation precessor - child head
			if (!precessor.equals("") && !childHead.equals("")) {
				this.getTemporalMap().put(precessor, childHead);
			}
			// create temporal relation child head - sucessor
			if (!sucessor.equals("") && !childTail.equals("")) {
				this.getTemporalMap().put(childTail, sucessor);
			}
		}

		if (node instanceof EventDeclaration)
			this.getEds().remove(node);
		else
			this.getEos().remove(node);
		// System.out.println("After removal: " + "\n" + this.provenanceMap + "\n" + this.temporalMap);
		// remove filters
		if (this.getFilters() != null) {
			if (this.getFilters().get(nodeId) != null) {
				this.getFilters().remove(nodeId);
			}
		}
		// remove selections
		// this.selections.removeAll(selsToRemove);
	}

	private void removeNodes(List<String> nodeIds) throws Exception {
		for (String s : nodeIds) {
			this.removeNode(s);
		}
	}

	public void removeSubTreeById(String rootId, boolean removeRoot) throws Exception {
		List<String> nodesToRemove = new ArrayList<String>();
		if (removeRoot)
			nodesToRemove.add(rootId);
		nodesToRemove.addAll(this.getDesendants(rootId));
		// System.out.println("removing tree: " + rootId + " Nodes in the Tree: " + nodesToRemove);
		this.removeNodes(nodesToRemove);
		// System.out.println("After tree removal: " + "\n" + this.provenanceMap + "\n" + this.temporalMap);
		// complete this method
	}

	/**
	 * @param edID
	 *            replace an event declaration with another without changing semantics of this pattern
	 * @throws Exception
	 * 
	 */
	public void replaceED(String edID, EventDeclaration replacement) throws Exception {
		boolean replacementIsPrimitive = false;
		if (replacement.getPayloads() != null)
			if (replacement.getPayloads().size() > 0)
				replacementIsPrimitive = true;
		if (!replacement.getnodeId().equals(replacement.getServiceId())) {// replacing leaf nodes in another pattern
			System.out.println("Not supposed to happen!");
			String parent = this.getParent(edID);
			String predecessor = this.getPredecessor(edID);
			String successor = this.getSuccessor(edID);
			List<Filter> filters = new ArrayList<Filter>();
			List<Selection> sels = this.getSelectionOnNode(edID);

			for (Selection sel : sels) {
				Selection selClone = sel.clone();
				selClone.setProvidedBy(replacement.getnodeId());
				// selClone.setOriginalED(replacement);
				this.getSelections().add(selClone);
			}
			if (this.filters != null)
				filters = this.filters.get(edID);
			if (parent != null)
				this.getProvenanceMap().get(parent).add(replacement.getnodeId());
			if (filters != null && filters.size() > 0)
				this.filters.put(replacement.getnodeId(), filters);
			this.removeNode(edID);
			if (predecessor != null)
				this.getTemporalMap().put(predecessor, replacement.getnodeId());
			if (successor != null)
				this.getTemporalMap().put(replacement.getnodeId(), successor);

			this.getEds().add(replacement);
		} else {// replacing event service description stored directly in repository
			List<Selection> sels = this.getSelectionOnNode(edID);

			for (Selection sel : sels) {
				// String pName = sel.getPropertyName();
				if (replacementIsPrimitive)
					for (String s : replacement.getPayloads()) {
						// logger.info("Replacing selection: " + sel);
						String pType = s.split("\\|")[0];
						String foi = s.split("\\|")[1];
						if (sel.getPropertyType().equals(pType)) {// && sel.getFoi().equals(foi)) {
							// logger.info("Replacing selection: " + pType + " , " + foi);
							sel.setPropertyName(s.split("\\|")[2]);
							sel.setFoi(foi);
							// sel.s
							sel.setOriginalED(replacement);
						}
						// logger.info("Replaced selection: " + sel);
					}
				else {
					List<Selection> sels1 = this.getSelectionOnNode(edID);
					List<Selection> sels2 = replacement.getEp().getSelections();
					for (Selection sel1 : sels1) {
						// boolean hasSub = false;
						for (Selection sel2 : sels2) {
							if (sel1.substitues(sel2)) {
								sel1.setPropertyName(sel2.getPropertyName());
								sel1.setOriginalED(sel2.getOriginalED());
							}
						}
					}
				}
				// this.getSelections().add(selClone);
			}
			this.replaceNodeId(edID, replacement.getnodeId());// change node id
			this.getEds().remove(this.getNodeById(replacement.getnodeId()));
			EventDeclaration replacementClone = replacement.clone();
			// replacementClone.setnodeId(edID);
			this.getEds().add(replacementClone);
		}
	}

	public boolean hasInconsistentSelection() {
		for (Selection sel : this.getSelections()) {
			EventDeclaration providedBy = (EventDeclaration) this.getNodeById(sel.getProvidedBy());
			EventDeclaration original = sel.getOriginalED();
			if (providedBy.getEp() == null)
				if (!providedBy.getServiceId().equals(original.getServiceId()))
					return true;
		}
		return false;
	}

	private void replaceNodeId(String originalId, String newId) {
		// logger.info("replacing: " + originalId + " , " + newId);
		Object node = this.getNodeById(originalId);
		if (node instanceof EventOperator) {
			((EventOperator) node).setID(newId);
		} else
			((EventDeclaration) node).setnodeId(newId);

		List<String> removeFromProv = new ArrayList<String>();
		List<String> addToProvAsValue = new ArrayList<String>();
		for (Entry e : this.provenanceMap.entrySet()) {
			if (e.getKey().equals(originalId)) {
				// this.provenanceMap.put(newId, (List) e.getValue());
				addToProvAsValue = (List) e.getValue();
				removeFromProv.add(originalId);
			} else if (((List<String>) e.getValue()).contains(originalId)) {
				((List<String>) e.getValue()).add(newId);
				((List<String>) e.getValue()).remove(originalId);
			}
		}
		for (String s : removeFromProv)
			this.provenanceMap.remove(s);
		this.provenanceMap.put(newId, addToProvAsValue);
		List<String> removeFromTemp = new ArrayList<String>();
		String addToTempAsValue = "";
		String addToTempAsKey = "";
		for (Entry e : this.temporalMap.entrySet()) {
			if (e.getKey().equals(originalId)) {
				addToTempAsValue = (String) e.getValue();
				removeFromTemp.add(originalId);
			} else if (e.getValue().equals(originalId)) {
				// this.temporalMap.put((String) e.getKey(), newId);
				addToTempAsKey = (String) e.getKey();
				// this.temporalMap.remove(e.getKey());
			}
		}
		this.temporalMap.put(newId, addToTempAsValue);
		this.temporalMap.put(addToTempAsKey, newId);
		for (String s : removeFromTemp)
			this.temporalMap.remove(s);
		for (Selection sel : this.selections) {
			if (sel.getProvidedBy().equals(originalId))
				sel.setProvidedBy(newId);
		}
		if (this.filters.containsKey(originalId)) {
			this.filters.put(newId, this.filters.get(originalId));
			this.filters.remove(originalId);
		}

	}

	/**
	 * @param nodeId
	 * @param replacement
	 * 
	 *            replaces a subtree with another without changing semantics
	 * @throws Exception
	 */
	public void replaceSubtree(String nodeId, EventPattern replacement) throws Exception {
		// Simulator2.logger.write("Replacing node: " + nodeId + "\n in: " + this.toString() + "\n with: "
		// + replacement.toString() + "\n");
		if (this.getNodeById(nodeId) != null) {
			String parent = this.getParent(nodeId);
			String predecessor = this.getPredecessor(nodeId);
			String successor = this.getSuccessor(nodeId);
			this.selections.removeAll(this.getSelectionOnNode(nodeId));
			String replacementRoot = replacement.getRootId();
			if (parent != null)
				this.getProvenanceMap().get(parent).add(replacementRoot);
			this.removeSubTreeById(nodeId, true);
			if (predecessor != null)
				this.getTemporalMap().put(predecessor, replacementRoot);
			if (successor != null)
				this.getTemporalMap().put(replacementRoot, successor);

			this.getEds().addAll(replacement.getEds());
			this.getEos().addAll(replacement.getEos());
			this.getFilters().putAll(replacement.getFilters());
			for (Selection sel : replacement.getSelections())
				this.getSelections().add(sel.clone());
			this.getProvenanceMap().putAll(replacement.getProvenanceMap());
			this.getTemporalMap().putAll(replacement.getTemporalMap());
			// List<Selection> sels = this.getSelectionOnNode(nodeId);
			// for (Selection sel : this.getSelectionOnNode(nodeId)) {
			//
			// }
		}
		// Simulator2.logger.write("Replaced results: " + this.toString() + "\n");
	}

	/**
	 * @param parentNodeId
	 * @param replacement
	 *            CAUTION: only applicable to replacing pure add patterns
	 * @throws Exception
	 */
	// public void replaceAndSubPattern(String parentNodeId, EventDeclaration replacement) throws Exception {
	// EventPattern completeReplacementPattern = replacement.getEp().getCompletePattern();
	// List<EventDeclaration> edsToRemove = new ArrayList<EventDeclaration>();
	// for (EventDeclaration ed : this.eds) {
	// if (ed.getEp() == null) {
	// if (completeReplacementPattern.hasSubstitutePrimitiveEd(ed))
	// edsToRemove.add(ed);
	// } else {
	// if (completeReplacementPattern.containsEd(ed))
	// edsToRemove.add(ed);
	// }
	// }
	// for (EventDeclaration ed : edsToRemove) {
	// this.provenanceMap.get(this.getRootId()).remove(ed.getnodeId());
	// this.selections.removeAll(this.getSelectionOnNode(ed.getnodeId()));
	// this.eds.remove(ed);
	// }
	// for (Selection sel : replacement.getEp().getSelections()) {
	// Selection selClone = sel.clone();
	// selClone.setProvidedBy(replacement.getnodeId());
	// this.getSelections().add(selClone);
	// }
	//
	// // for (EventDeclaration ed : replacement.getEds()) {
	// this.provenanceMap.get(this.getRootId()).add(replacement.getnodeId());
	// this.eds.add(replacement);
	// // this.replaceNodeId(ed.getnodeId(), "ED-" + UUID.randomUUID());
	// // }
	// // logger.info("replacement result: " + this.toSimpleString());
	//
	// }

	// private boolean hasSubstitutePrimitiveEd(EventDeclaration externalEd) {
	// for (EventDeclaration ed : this.getEds()) {
	// if (Comparator.isSubstitutePrimitiveEvent(ed, externalEd))
	// return true;
	// }
	// return false;
	// }

	// private boolean containsEd(EventDeclaration ed) throws Exception {
	// if (ed.getEp() == null)
	// throw new Exception("Performing containing relation analysis over primitive events.");
	// for (EventDeclaration subEd : ed.getEp().getCompletePattern().getEds()) {
	// if (!this.hasSubstitutePrimitiveEd(subEd))
	// return false;
	// }
	// return true;
	// }

	/**
	 * @param repOrSeqId
	 *            recursively folds repeating child sequences for a given repetition or sequence operator
	 * @throws Exception
	 */
	// private void sequenceFold(List<String> sortedChildIds) throws Exception {
	// // TODO Algorithm needs review
	// // System.out.println("Folding: " + sortedChildIds + " idcnt: " + this.idCnt);
	// // System.out.println("Folding before: " + this.provenanceMap + "\n" + this.temporalMap);
	// if (sortedChildIds.size() > 1) {
	// int maxLength = sortedChildIds.size() / 2;
	// // System.out.println("maxlength: " + maxLength);
	// for (int length = maxLength; length > 0; length--) {
	// boolean folded = false;
	// // System.out.println("length: " + length);
	// for (int i = 0; i < sortedChildIds.size() - (2 * length) + 1; i++) {
	// // System.out.println("length: " + length);
	// List<String> subSequence = new ArrayList<String>();
	// for (int j = i; j < i + length; j++)
	// subSequence.add(sortedChildIds.get(j));
	// String indexStr = this.getRepeatingIndex(subSequence, sortedChildIds, i, i + length - 1);
	// if (indexStr != null) {
	//
	// int start = Integer.parseInt(indexStr.split("-")[0]);
	// int end = Integer.parseInt(indexStr.split("-")[1]);
	// // System.out.println("Found repeating:" + subSequence + " in " + sortedChildIds + " " + start
	// // + " " + end);
	// // insert rep node for the sequence
	// int cardinality = (end - start + 1) / length;
	//
	// EventOperator rep = new EventOperator(OperatorType.rep, cardinality, (this.idCnt + 1) + "");
	// // for(int k=start;k<end+1;k++){
	// //
	// // }
	//
	// for (int k = start + length; k < end + 1; k++) {
	// // System.out.println("removing tree: " + sortedChildIds.get(k));
	// this.removeSubTreeById(sortedChildIds.get(k), true);
	// }
	// // System.out.println("removing completed! ");
	// this.insertParent(rep, subSequence);
	// // parse remaining head sequence
	// if (start > 1) {
	// List<String> headSequence = new ArrayList<String>();
	// for (int k = 0; k < start; k++) {
	// headSequence.add(sortedChildIds.get(k));
	// }
	// this.sequenceFold(headSequence);
	// }
	// // parse remaining tail sequence
	// if (end < sortedChildIds.size() - 2) {
	// List<String> tailSequence = new ArrayList<String>();
	// for (int k = end + 1; k < sortedChildIds.size(); k++) {
	// tailSequence.add(sortedChildIds.get(k));
	// }
	// this.sequenceFold(tailSequence);
	// }
	//
	// // continue to find subsequences of the subsequence
	// if (length > 1) {
	// List<String> newSequence = new ArrayList<String>();
	// for (int k = start; k < start + length; k++)
	// newSequence.add(sortedChildIds.get(k));
	// this.sequenceFold(newSequence);
	// }
	// folded = true;
	// break;
	// }
	// }
	// if (folded)
	// break;
	// }
	// }
	// // System.out.println("Folding after: " + this.provenanceMap + "\n" + this.temporalMap);
	// }

	/**
	 * @param repId
	 * @param cardinality
	 * @throws Exception
	 * 
	 *             unfolds the repetition operator
	 */
	private void sequenceReplicate(String repId, int cardinality) throws Exception {
		// TODO offset debuging
		// System.out.println("Replication before: " + this.provenanceMap + "\n" + this.temporalMap);
		List<String> nodesToRemove = new ArrayList<String>();
		nodesToRemove.add(repId);
		EventPattern subtreeReplica1 = (EventPattern) this.getSubtreeByRoot(repId);
		// int initialOffset = this.idCnt - Integer.parseInt(repId) + 1;
		for (int i = 1; i < cardinality; i++) {
			EventPattern subtreeReplica = (EventPattern) subtreeReplica1.clone();
			int offset = this.idCnt + 1;

			// System.out.println("Creating replica for " + repId + ": " + subtreeReplica.toString());
			subtreeReplica.applyOffset(offset);
			// System.out.println("Creating replica for " + repId + ": " + subtreeReplica.toString());
			// System.out.println(subtreeReplica.getTemporalMap());
			this.getEds().addAll(subtreeReplica.getEds());
			this.getEos().addAll(subtreeReplica.getEos());
			this.getProvenanceMap().putAll(subtreeReplica.getProvenanceMap());
			this.getTemporalMap().putAll(subtreeReplica.getTemporalMap());
			String subRoot = subtreeReplica.getRootId();
			String tail = this.getTemporalMap().get(repId);
			String parent = this.getParent(repId);
			this.getProvenanceMap().get(parent).add(subRoot);
			this.getTemporalMap().put(repId, subRoot);
			if (tail != null)
				this.getTemporalMap().put(subRoot, tail);
			nodesToRemove.add(subRoot);
			this.idCnt += (subtreeReplica.getSize());
			// System.out.println("After replication: " + this.getID() + "\n" + this.provenanceMap + "\n"
			// + this.temporalMap);// increase node count after replication
		}
		this.removeNodes(nodesToRemove);
		// System.out.println("Replication after: " + this.provenanceMap + "\n" + this.temporalMap);
	}

	public void setEds(List<EventDeclaration> eds) {
		this.eds = eds;
	}

	public void setEos(List<EventOperator> eos) {
		this.eos = eos;
	}

	public void setFilters(Map<String, List<Filter>> filters) {
		this.filters = filters;
	}

	public void setID(String iD) {
		ID = iD;
	}

	public void setIdCnt(int idCnt) {
		this.idCnt = idCnt;
	}

	public void setProvenanceMap(Map<String, List<String>> provenanceMap) {
		this.provenanceMap = provenanceMap;
	}

	public void setQuery(boolean isQuery) {
		this.isQuery = isQuery;
	}

	public void setSelections(List<Selection> selections) {
		this.selections = selections;
	}

	public void setTemporalMap(Map<String, String> temporalMap) {
		this.temporalMap = temporalMap;
	}

	public void setTimeWindow(int timeWindow) {
		this.timeWindow = timeWindow;
	}

	public void setTrafficDemand(int trafficDemand) {
		this.trafficDemand = trafficDemand;
	}

	public List<EventPattern> sortSequenceChildPatterns(List<EventPattern> childEps) throws NodeRemovalException {
		List<EventPattern> result = new ArrayList<EventPattern>();
		List<String> childIds = new ArrayList<String>();
		for (EventPattern ep : childEps)
			childIds.add(ep.getRootId());
		List<String> sortedIds = this.sortSequenceChilds(childIds);
		for (String id : sortedIds) {
			for (EventPattern ep : childEps) {
				if (ep.getRootId().equals(id)) {
					result.add(ep);
					break;
				}
			}
		}
		return result;

	}

	public List<EventPattern> getSortedDirectSubtrees() throws CloneNotSupportedException, NodeRemovalException {
		List<EventPattern> dsts = this.getDirectSubtrees();
		return this.sortSequenceChildPatterns(dsts);
	}

	public List<String> sortSequenceChilds(List<String> childIds) throws NodeRemovalException {
		String first = "";
		// System.out.println("in: " + childIds);
		for (String s : childIds) {
			if (this.getTemporalMap().containsValue(s)) {
				Iterator<Entry<String, String>> it = this.getTemporalMap().entrySet().iterator();
				while (it.hasNext()) {
					Entry<String, String> e = it.next();
					if (e.getValue().equals(s) && !childIds.contains(e.getKey()))
						first = s;
				}
			} else
				first = s;
		}
		// System.out.println("Found first node during sort: " + first);
		ArrayList<String> result = new ArrayList<String>();
		result.add(first);
		while (result.size() != childIds.size()) {
			String lastSorted = result.get(result.size() - 1);
			if (this.getTemporalMap().get(lastSorted) != null)
				result.add(this.getTemporalMap().get(lastSorted));
			else {
				// System.out.println("no temp rel found for: " + lastSorted + " in " + this.getID() + "\n" +
				// "Temporal: "
				// + this.getTemporalMap());
				throw new NodeRemovalException(childIds.toString());
			}
		}
		// System.out.println("out: " + result);
		return result;
	}

	public String toString() {
		String result;
		try {
			String sel = "";
			if (this.getSelections() != null) {
				sel = this.getSelections().toString();
			}
			result = this.getID() + "(" + this.timeWindow + ")" + "=" + this.getNodeStr(this.getRootId());
			// + " Filters: " + this.filters;
			if (this.getSelections() != null)
				result = result + "\n Selection: " + sel;
			result = result.replaceAll("http://www.insight-centre.org/dataset/SampleEventService", "dataset")
					.replaceAll("http://www.insight-centre.org/ontologies", "onto")
					.replaceAll("http://www.insight-centre.org/citytraffic", "ct");
			return result;
		} catch (NodeRemovalException e) {

			e.printStackTrace();
			System.exit(0);
		}
		return null;

	}

	public String toSimpleString() {
		String result = "";
		result = this.getID() + "(";
		for (EventDeclaration ed : this.eds)
			result += ed.getServiceId() + " ";
		result += ")";
		return result.replaceAll("http://www.insight-centre.org/dataset/SampleEventService", "dataset");
	}

	public void updateIdCnt() {
		int largest = -1;
		for (EventDeclaration ed : this.getEds()) {
			if (largest <= Integer.parseInt(ed.getnodeId()) || largest < 0) {
				largest = Integer.parseInt(ed.getnodeId());
			}
		}
		for (EventOperator eo : this.getEos()) {
			if (largest <= Integer.parseInt(eo.getID()) || largest < 0) {
				largest = Integer.parseInt(eo.getID());
			}
		}
		this.setIdCnt(largest + 1);
	}

	public EventPattern variateIds() {
		this.setID("EP-" + UUID.randomUUID());
		for (EventOperator eo : this.eos) {
			String originalId = eo.getID();
			String newId = "EO-" + UUID.randomUUID() + "";
			this.replaceNodeId(originalId, newId);
		}
		for (EventDeclaration ed : this.eds) {
			String originalId = ed.getnodeId();
			String newId = "ED-" + UUID.randomUUID() + "";
			this.replaceNodeId(originalId, newId);
		}
		return this;
	}

	@Override
	public int compareTo(EventPattern arg0) {
		if (this.getSize() < arg0.getSize())
			return -1;
		if (this.getSize() > arg0.getSize())
			return 1;
		return 0;
	}
}
