package org.insight_centre.aceis.io;

import com.csvreader.CsvReader;

import java.io.*;
import java.lang.Double;
import java.lang.String;
import java.lang.System;
import java.util.ArrayList;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.String;
import java.lang.System;

import org.insight_centre.aceis.observations.AarhusTrafficObservation;

import com.csvreader.CsvReader;

/**
 * Created by sefki on 10/06/2014.
 */
public class ReadDocument {

	public static void main(String[] args) throws IOException {
		StreamAnnotation();

	}

	public static AarhusTrafficObservation getStreamData(String dataFile) throws NumberFormatException, IOException {
		CsvReader streamData = new CsvReader(String.valueOf(dataFile));
		streamData.readHeaders();
		// Reads csv document for traffic metadata
		try {
			CsvReader metaData = new CsvReader("dataset/MetaData/trafficMetaData.csv");
			metaData.readHeaders();
			AarhusTrafficObservation data = new AarhusTrafficObservation();
			while (streamData.readRecord()) {

				while (metaData.readRecord()) {
					if (streamData.get("REPORT_ID").equals(metaData.get("REPORT_ID"))) {
						data = new AarhusTrafficObservation(Double.parseDouble(streamData.get("REPORT_ID")),
								Double.parseDouble(streamData.get("avgSpeed")), Double.parseDouble(streamData
										.get("vehicleCount")), Double.parseDouble(streamData.get("avgMeasuredTime")),
								0, 0, metaData.get("POINT_1_STREET"), metaData.get("POINT_1_CITY"),
								Double.parseDouble(metaData.get("POINT_1_LAT")), Double.parseDouble(metaData
										.get("POINT_1_LNG")), metaData.get("POINT_2_STREET"),
								metaData.get("POINT_2_CITY"), Double.parseDouble(metaData.get("POINT_2_LAT")),
								Double.parseDouble(metaData.get("POINT_2_LNG")), metaData.get("POINT_1_COUNTRY"),
								metaData.get("POINT_2_COUNTRY"), metaData.get("TIMESTAMP"));
						Double distance = Double.parseDouble(metaData.get("DISTANCE_IN_METERS"));
						data.setEstimatedTime(distance / data.getAverageSpeed());
						data.setCongestionLevel(data.getVehicle_count() / distance);
					}
				}

			}
			streamData.close();
			metaData.close();
			return data;
		} catch (Exception e) {

			e.printStackTrace();
		}
		return null;
	}

	public static void StreamAnnotation() {
		ArrayList<AarhusTrafficObservation> List = new ArrayList<AarhusTrafficObservation>();
		try {

			File StreamDataDirectory = new File("dataset/Data/");
			File StreamDataFiles[] = StreamDataDirectory.listFiles();

			for (File csvStreamFile : StreamDataFiles) {
				// Reads csv document for trafficdata
				CsvReader streamData = new CsvReader(String.valueOf(csvStreamFile));
				streamData.readHeaders();
				// Reads csv document for traffic metadata
				CsvReader metaData = new CsvReader("dataset/MetaData/trafficMetaData.csv");
				metaData.readHeaders();

				while (streamData.readRecord()) {
					while (metaData.readRecord()) {
						if (streamData.get("REPORT_ID").equals(metaData.get("REPORT_ID"))) {
							List.add(new AarhusTrafficObservation(Double.parseDouble(streamData.get("REPORT_ID")),
									Double.parseDouble(streamData.get("avgSpeed")), Double.parseDouble(streamData
											.get("vehicleCount")),
									Double.parseDouble(streamData.get("avgMeasuredTime")), 0, 0, metaData
											.get("POINT_1_STREET"), metaData.get("POINT_1_CITY"), Double
											.parseDouble(metaData.get("POINT_1_LAT")), Double.parseDouble(metaData
											.get("POINT_1_LNG")), metaData.get("POINT_2_STREET"), metaData
											.get("POINT_2_CITY"), Double.parseDouble(metaData.get("POINT_2_LAT")),
									Double.parseDouble(metaData.get("POINT_2_LNG")), metaData.get("POINT_1_COUNTRY"),
									metaData.get("POINT_2_COUNTRY"), metaData.get("TIMESTAMP")));

						}
					}

				}
				streamData.close();
				metaData.close();
			}

			// Prints created StreamData instances in order to test the codes.
			for (AarhusTrafficObservation stream1 : List) {
				System.out.println(stream1);
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
