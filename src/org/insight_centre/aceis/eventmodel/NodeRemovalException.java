package org.insight_centre.aceis.eventmodel;

@SuppressWarnings("serial")
public class NodeRemovalException extends Exception {
	// private String nodeId;

	public NodeRemovalException(String nodeId) {
		super("Removing node: " + nodeId + " will cause erroenous event semantics.");
	}
}
