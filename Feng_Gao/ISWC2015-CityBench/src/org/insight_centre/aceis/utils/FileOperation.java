package org.insight_centre.aceis.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class FileOperation {
	public static void main(String[] args) throws IOException {
		// File trafficRawDirectory = new File("streams");
		// File[] trafficRawFiles = trafficRawDirectory.listFiles();
		// for (File tr : trafficRawFiles) {
		// System.out.println("renaming: " + tr.toPath());
		// File newTr;
		// if (tr.getName().contains("traffic")) {
		// newTr = new File("streams" + File.separator + "AarhusT"
		// + tr.getName().substring(1, tr.getName().length()));
		// tr.renameTo(newTr);
		// } else if (tr.getName().contains("pollution")) {
		// newTr = new File("streams" + File.separator + "AarhusP"
		// + tr.getName().substring(1, tr.getName().length()));
		// tr.renameTo(newTr);
		// }
		//
		// // Files.copy(tr.toPath(), newTr.toPath());
		// }
		CsvReader streamData = new CsvReader("dataset/aarhus_parking.csv");
		streamData.setTrimWhitespace(false);
		streamData.setDelimiter(',');
		streamData.readHeaders();

		List<String> streamNames = new ArrayList<String>();
		streamNames.add("NORREPORT");
		streamNames.add("BUSGADEHUSET");
		streamNames.add("BRUUNS");
		streamNames.add("SKOLEBAKKEN");
		streamNames.add("SCANDCENTER");
		streamNames.add("SALLING");
		streamNames.add("MAGASIN");
		streamNames.add("KALKVAERKSVEJ");

		Map<String, CsvWriter> writerMap = new HashMap<String, CsvWriter>();
		for (String s : streamNames)
			try {
				// use FileWriter constructor that specifies open for appending
				String outputFile = "dataset/AarhusParkingData-" + s + ".stream";
				boolean alreadyExists = new File(outputFile).exists();
				CsvWriter csvOutput = new CsvWriter(new FileWriter(outputFile, true), ',');

				// if the file didn't already exist then we need to write out the header line
				if (!alreadyExists) {
					csvOutput.write("vehiclecount");
					csvOutput.write("updatetime");
					csvOutput.write("_id");
					csvOutput.write("totalspaces");
					csvOutput.write("garagecode");
					csvOutput.write("streamtime");
					csvOutput.endRecord();
				}
				// }
				writerMap.put(s, csvOutput);
				// csvOutput.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		while (streamData.readRecord()) {
			String code = streamData.get("garagecode");
			writerMap.get(code).write(streamData.get("vehiclecount"));
			writerMap.get(code).write(streamData.get("updatetime"));
			writerMap.get(code).write(streamData.get("_id"));
			writerMap.get(code).write(streamData.get("totalspaces"));
			writerMap.get(code).write(streamData.get("garagecode"));
			writerMap.get(code).write(streamData.get("streamtime"));
			writerMap.get(code).endRecord();
		}
		for (CsvWriter csv : writerMap.values())
			csv.close();
	}
}
