package org.insight_centre.aceis.io.rdf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.insight_centre.aceis.eventmodel.*;
import org.insight_centre.aceis.io.EventRepository;

/**
 * @author feng
 * 
 *         String based I/O module
 * 
 */
public class TextFileManager {
	public static final String filePathPrefix = "patterns/";

	public static EventRepository buildRepoFromFile(String fileName, String fileName2) throws IOException {
		File file = new File(filePathPrefix + fileName);
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		List<EventPattern> eps = readPatternFromFile(fileName2);
		// File file2 = new File(filePathPrefix + fileName2);
		// FileReader fr2 = new FileReader(file2);
		// BufferedReader br2 = new BufferedReader(fr2);
		Map<String, EventPattern> epMap = new HashMap<String, EventPattern>();
		for (EventPattern ep : eps)
			epMap.put(ep.getID(), ep);
		EventRepository repo = new EventRepository();
		String epStr = br.readLine();
		// List<EventPattern> results = new ArrayList<EventPattern>();
		while (epStr != null) {
			String prefix = epStr.split("=")[0];

			if (prefix.contains("EP-")) {
				EventPattern result = TextFileManager.stringToPattern(epStr);
				repo.getEps().put(result.getID(), result);
				// System.out.println("Adding: "+result.getID()+": "+result.toString());
			} else if (prefix.contains("ED-")) {
				EventDeclaration result = TextFileManager.stringToED(epStr, epMap);
				repo.getEds().put(result.getnodeId(), result);
				// System.out.println("Adding: "+result.getID()+": "+result.toString());
			}
			// System.out.println("added: " + result.toString());
			epStr = br.readLine();
		}
		br.close();
		// repo.buildDependencies();
		return repo;

	}

	private static int findPairwiseParethesis(String epStr, int startIndex) {
		Stack<Integer> stack = new Stack<Integer>();
		// stack.push(startIndex);
		int result = -1;
		for (int i = startIndex; i < epStr.length(); i++) {
			if ((epStr.charAt(i) + "").equals("("))
				stack.push(i);
			else if ((epStr.charAt(i) + "").equals(")"))
				stack.pop();
			if (stack.isEmpty()) {
				result = i;
				break;
			}
		}
		return result;
	}

	private static Double getFrequencyFromStr(String epStr) {
		String result = epStr.split("=")[0].split("\\(")[1];
		result = result.substring(0, result.length() - 1);
		return Double.parseDouble(result);
	}

	private static String getIDFromStr(String epStr) {
		String s = epStr.split("=")[0].split("\\(")[0];
		return s.split("-")[1];
	}

	private static int getTimeWindowFromStr(String epStr) {
		String result = epStr.split("=")[0].split("\\(")[1];
		result = result.substring(0, result.length() - 1);
		return Integer.parseInt(result);
	}

	private static int getTrafficDemandFromStr(String epStr) {
		String result = epStr.split("=")[0].split("\\(")[1].split(",")[1];
		result = result.substring(0, result.length() - 1);
		return Integer.parseInt(result);
	}

	private static Object makeNode(String epStr) {

		String typeCard = epStr.split("-")[0];
		String type = typeCard.split(":")[0];
		int card = 1;
		// Double freq = 0.0;
		// if (typeCard.split(":").length > 1)
		// card = Integer.parseInt(typeCard.split(":")[1]);
		String id = epStr.split("-")[1];
		if (type.equals("and"))
			return new EventOperator(OperatorType.and, card, id);
		else if (type.equals("or"))
			return new EventOperator(OperatorType.or, card, id);
		else if (type.equals("seq"))
			return new EventOperator(OperatorType.seq, card, id);
		else if (type.equals("rep")) {
			card = Integer.parseInt(typeCard.split(":")[1]);
			return new EventOperator(OperatorType.rep, card, id);
		} else {
			return stringToED(epStr, null);
		}
	}

	private static Object parseNodeStr(EventPattern result, String epStr) {
		// List<String> eds = new ArrayList<String>();
		Object node = TextFileManager.makeNode(epStr.split("\\(")[0]);
		if (node instanceof EventOperator)
			result.getEos().add((EventOperator) node);
		else {
			EventDeclaration leafNode = (EventDeclaration) node;
			// if (leafNode.getEventType() == null)
			// eds.add(((EventDeclaration) node).getID());
			// else
			result.getEds().add(leafNode);
		}
		// TextFileManager.addO(result, node);
		String childStr = epStr.substring(epStr.split("\\(")[0].length());
		List<String> childStrs = TextFileManager.splitChildStr(childStr);
		List<String> childIds = new ArrayList<String>();
		if (childStrs.size() > 0) {
			EventOperator parent = (EventOperator) node; // Asserting it's an operator!
			for (String s : childStrs) {
				String childId = "";
				Object child = TextFileManager.parseNodeStr(result, s);
				if (child instanceof EventOperator)
					childId = ((EventOperator) child).getID();
				else {
					childId = ((EventDeclaration) child).getnodeId();
					// if (!eds.contains(childId))
					// eds.add(childId);
				}
				if (result.getProvenanceMap().get(parent.getID()) == null)
					result.getProvenanceMap().put(parent.getID(), new ArrayList<String>());
				result.getProvenanceMap().get(parent.getID()).add(childId);
				childIds.add(childId);
			}
			for (int i = 0; i < childIds.size(); i++) {
				if ((parent.getOpt() == OperatorType.rep || parent.getOpt() == OperatorType.seq) && i > 0)
					result.getTemporalMap().put(childIds.get(i - 1), childIds.get(i));
			}
		}

		return node;
	}

	public static List<EventPattern> readPatternFromFile(String fileName) throws IOException {
		File file = new File(filePathPrefix + fileName);
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		String epStr = br.readLine();
		List<EventPattern> results = new ArrayList<EventPattern>();
		while (epStr != null) {
			// System.out.println("parsing: " + epStr);
			EventPattern result = TextFileManager.stringToPattern(epStr);
			results.add(result);
			// System.out.println("added: " + result.toString());
			epStr = br.readLine();
		}
		br.close();
		return results;

	}

	private static List<String> splitChildStr(String epStr) {
		List<String> results = new ArrayList<String>();
		if (epStr.equals(""))
			return results;
		if ((epStr.charAt(0) + "").equals("(") && (epStr.charAt(epStr.length() - 1) + "").equals(")"))
			epStr = epStr.substring(1, epStr.length() - 1);
		while (epStr.length() > 0) {
			String firstSegment = epStr.split(",")[0];
			if (firstSegment.contains("(")) {
				int startIndex = firstSegment.indexOf("(");
				int endIndex = TextFileManager.findPairwiseParethesis(epStr, startIndex);
				firstSegment = epStr.substring(0, endIndex + 1);
				// epStr=epStr.replace(target, sment)
			}
			results.add(firstSegment);
			epStr = epStr.substring(firstSegment.length());
			if (epStr.length() > 0)
				if ((epStr.charAt(0) + "").equals(","))
					epStr = epStr.substring(1);
		}

		return results;
	}

	public static EventDeclaration stringToED(String edStr, Map<String, EventPattern> map) {
		EventDeclaration ed = new EventDeclaration("", "", "", null, null, null);
		List<String> ep = new ArrayList<String>();
		ed.setnodeId(TextFileManager.getIDFromStr(edStr));
		// ed.setFrequency(TextFileManager.getFrequencyFromStr(edStr));
		String[] atts = edStr.split("=")[1].split(";");
		QosVector qos = new QosVector(0, 0, 0, 0.0, 0.0, 0.0);
		for (int i = 0; i < atts.length; i++) {
			String att = atts[i].split(":")[0];
			String value = atts[i].split(":")[1];
			if (att.equals("T")) {
				if (value.contains("EP-")) {
					ed.setEventType("complex");
					ed.setEp(map.get(value.split("-")[1]));
					// map.put(ed.getID(), eps);
				} else
					ed.setEventType(value);
			} else if (att.equals("F"))
				ed.setFrequency(Double.parseDouble(value));
			else if (att.equals("L"))
				qos.setLatency(Integer.parseInt(value));
			else if (att.equals("P"))
				qos.setPrice(Integer.parseInt(value));
			else if (att.equals("S"))
				qos.setSecurity(Integer.parseInt(value));
			else if (att.equals("A"))
				qos.setAccuracy(Double.parseDouble(value));
			else
				qos.setReliability(Double.parseDouble(value));
		}
		ed.setInternalQos(qos);
		return ed;
	}

	public static EventPattern stringToPattern(String epStr) {
		EventPattern result = new EventPattern("", new ArrayList<EventDeclaration>(), new ArrayList<EventOperator>(),
				new HashMap<String, List<String>>(), null, 0, 0);
		result.setID(TextFileManager.getIDFromStr(epStr));
		result.setTimeWindow(TextFileManager.getTimeWindowFromStr(epStr));
		// result.setTrafficDemand(TextFileManager.getTrafficDemandFromStr(epStr));
		String nodeStr = epStr.substring(epStr.split("=")[0].length() + 1, epStr.length());
		// ArrayList<String> eds = new ArrayList<String>();
		TextFileManager.parseNodeStr(result, nodeStr);
		// result.updateIdCnt();
		// map.put(result.getID(), eds);
		return result;

	}

	public static void writeEDToFile(String fileName, String file2, List<EventPattern> patterns,
			List<EventDeclaration> eds) throws IOException {
		File newFile = new File(filePathPrefix + fileName);
		FileWriter fw = new FileWriter(newFile);
		BufferedWriter bw = new BufferedWriter(fw);

		File newFile2 = new File(filePathPrefix + file2);
		FileWriter fw2 = new FileWriter(newFile2);
		BufferedWriter bw2 = new BufferedWriter(fw2);
		for (EventPattern ep : patterns) {
			if (ep.getSize() == 1)
				continue;
			bw.write(ep.toString());
			bw.newLine();
		}
		for (EventDeclaration ed : eds) {
			bw.write(ed.toString());
			bw.newLine();
			if (ed.getEp() != null) {
				bw2.write(ed.getEp().toString());
				bw2.newLine();
			}
		}
		bw.close();
		bw2.close();
	}

	public static void writePatternToFile(String fileName, List<EventPattern> patterns) throws IOException {
		File newFile = new File(filePathPrefix + fileName);
		FileWriter fw = new FileWriter(newFile);
		BufferedWriter bw = new BufferedWriter(fw);
		for (EventPattern ep : patterns) {
			bw.write(ep.toString());
			bw.newLine();
		}
		bw.close();
	}
}
