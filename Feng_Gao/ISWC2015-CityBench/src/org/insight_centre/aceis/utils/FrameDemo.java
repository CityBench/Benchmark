package org.insight_centre.aceis.utils;

import javax.swing.JFrame;

//package org.insight_centre.aceis.utils;

import java.awt.*;
import java.awt.event.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.swing.*;

import org.insight_centre.aceis.io.rdf.RDFFileManager;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.vocabulary.RDF;

/* FrameDemo.java requires no other files. */
public class FrameDemo {
	/**
	 * Create the GUI and show it. For thread safety, this method should be invoked from the event-dispatching thread.
	 */
	private static void createAndShowGUI() {
		// Create and set up the window.
		JFrame frame = new JFrame("FrameDemo");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		JLabel emptyLabel = new JLabel("");
		emptyLabel.setPreferredSize(new Dimension(175, 100));
		frame.getContentPane().add(emptyLabel, BorderLayout.CENTER);

		// Display the window.
		frame.pack();
		frame.setVisible(true);
	}

	public static void main(String[] args) throws IOException {
		// Schedule a job for the event-dispatching thread:
		// creating and showing this application's GUI.
		Model m = FileManager.get().loadModel("dataset/aarhusLibraryEvents.n3");
		ResIterator it = m.listSubjectsWithProperty(RDF.type, m.createResource("http://purl.oclc.org/NET/sao/Point"));
		while (it.hasNext()) {
			it.next().addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));
		}
		File f = new File("dataset/library_events.n3");
		m.write(new FileWriter(f), "N3");
	}
}
