package org.insight_centre.aceis.utils;

import javax.swing.JFrame;

//package org.insight_centre.aceis.utils;

import java.awt.*;
import java.awt.event.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;

import javax.swing.*;

import org.insight_centre.aceis.io.rdf.RDFFileManager;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
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
		Model m = FileManager.get().loadModel("dataset/AarhusCulturalEvents.n3");
		StmtIterator it = m.listStatements();
		// ResIterator it = m.listSubjectsWithProperty(RDF.type,
		// m.createResource("http://purl.oclc.org/NET/sao/Point"));
		Model newmodel = ModelFactory.createDefaultModel();
		while (it.hasNext()) {
			Statement stmt = it.next();
			Statement newStmt;
			if (stmt.getPredicate().toString().contains("hasLatitude")) {
				double lat = stmt.getObject().asLiteral().getDouble();
				Literal l = newmodel.createTypedLiteral(lat);
				newStmt = ResourceFactory.createStatement(stmt.getSubject(), stmt.getPredicate(), l);
			} else if (stmt.getPredicate().toString().contains("hasLongitude")) {
				double lon = stmt.getObject().asLiteral().getDouble();
				Literal l = newmodel.createTypedLiteral(lon);
				newStmt = ResourceFactory.createStatement(stmt.getSubject(), stmt.getPredicate(), l);
			} else if (stmt.getPredicate().toString().contains("value")) {
				Literal l = newmodel.createTypedLiteral(UUID.randomUUID() + "");
				newStmt = ResourceFactory.createStatement(stmt.getSubject(), stmt.getPredicate(), l);
			} else {
				newStmt = ResourceFactory.createStatement(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
			}
			newmodel.add(newStmt);
		}

		File f = new File("dataset/AarhusCulturalEvents.rdf");
		newmodel.write(new FileWriter(f), "RDF/XML");
	}
}
