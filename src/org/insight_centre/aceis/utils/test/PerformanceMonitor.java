package org.insight_centre.aceis.utils.test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.insight_centre.aceis.io.streams.cqels.CQELSResultListener;
import org.insight_centre.aceis.io.streams.cqels.CQELSSensorStream;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLResultObserver;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLSensorStream;
import org.insight_centre.aceis.observations.SensorObservation;
import org.insight_centre.citybench.main.CityBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvWriter;

public class PerformanceMonitor implements Runnable {
	private Map<String, String> qMap;
	private long duration;
	private int duplicates;
	private String resultName;
	private long start = 0;
	private ConcurrentHashMap<String, List<Long>> latencyMap = new ConcurrentHashMap<String, List<Long>>();
	private List<Double> memoryList = new ArrayList<Double>();;
	private ConcurrentHashMap<String, Long> resultCntMap = new ConcurrentHashMap<String, Long>();
	private CsvWriter cw;
	private long resultInitTime = 0, lastCheckPoint = 0, globalInit = 0;
	private boolean stop = false;
	private List<String> qList;
	private static final Logger logger = LoggerFactory.getLogger(PerformanceMonitor.class);

	public PerformanceMonitor(Map<String, String> queryMap, long duration, int duplicates, String resultName)
			throws Exception {
		qMap = queryMap;
		this.duration = duration;
		this.resultName = resultName;
		this.duplicates = duplicates;
		File outputFile = new File("result_log" + File.separator + resultName + ".csv");
		if (outputFile.exists())
			throw new Exception("Result log file already exists.");
		cw = new CsvWriter(new FileWriter(outputFile, true), ',');
		cw.write("");
		qList = new ArrayList(this.qMap.keySet());
		Collections.sort(qList);

		for (String qid : qList) {
			latencyMap.put(qid, new ArrayList<Long>());
			resultCntMap.put(qid, (long) 0);
			cw.write("latency-" + qid);
		}
		// for (String qid : qList) {
		// cw.write("cnt-" + qid);
		// }
		cw.write("memory");
		cw.endRecord();
		// cw.flush();
		// cw.
		this.globalInit = System.currentTimeMillis();
	}

	public void run() {
		int minuteCnt = 0;
		while (!stop) {
			try {
				if (((System.currentTimeMillis() - this.globalInit) > 1.5 * duration)
						|| (duration != 0 && resultInitTime != 0 && (System.currentTimeMillis() - this.resultInitTime) > (30000 + duration))) {
					this.cw.flush();
					this.cw.close();
					logger.info("Stopping after " + (System.currentTimeMillis() - this.globalInit) + " ms.");
					this.cleanup();
					logger.info("Experimment stopped.");
					System.exit(0);
				}

				if (this.lastCheckPoint != 0 && (System.currentTimeMillis() - this.lastCheckPoint) >= 60000) {
					minuteCnt += 1;

					this.lastCheckPoint = System.currentTimeMillis();
					cw.write(minuteCnt + "");
					for (String qid : this.qList) {
						double latency = 0.0;
						for (long l : this.latencyMap.get(qid))
							latency += l;
						latency = (latency + 0.0) / (this.latencyMap.get(qid).size() + 0.0);
						cw.write(latency + "");

					}
					// for (String qid : this.qList)
					// cw.write((this.resultCntMap.get(qid) / (this.duplicates + 0.0)) + "");
					double memory = 0.0;
					for (double m : this.memoryList)
						memory += m;
					memory = memory / (this.memoryList.size() + 0.0);
					cw.write(memory + "");
					cw.endRecord();
					cw.flush();
					logger.info("Results logged.");

					// empty memory and latency lists
					this.memoryList.clear();
					for (Entry<String, List<Long>> en : this.latencyMap.entrySet()) {
						en.getValue().clear();
					}
				}

				Map<String, Double> currentLatency = new HashMap<String, Double>();
				for (String qid : this.qList) {
					double latency = 0.0;
					for (long l : this.latencyMap.get(qid))
						latency += l;
					latency = (latency + 0.0) / (this.latencyMap.get(qid).size() + 0.0);
					currentLatency.put(qid, latency);
				}

				// Map<String,Long> currentResults=new HashMap<String>

				// ConcurrentHashMap<String, SensorObservation> obMapBytes = CityBench.obMap;
				// double obMapBytes = 0.0;
				// try {
				// ByteArrayOutputStream baos = new ByteArrayOutputStream();
				// ObjectOutputStream oos = new ObjectOutputStream(baos);
				// oos.writeObject(CityBench.obMap);
				// oos.close();
				// obMapBytes = (0.0 + baos.size());
				// } catch (Exception e) {
				// e.printStackTrace();
				// }
				// long listerObIdListBytes = 0;
				// for (Object listener : CityBench.registeredQueries.values()) {
				//
				// if (listener instanceof CQELSResultListener) {
				// for (String obid : ((CQELSResultListener) listener).capturedObIds)
				// listerObIdListBytes += obid.getBytes().length;
				// } else {
				// for (String obid : ((CSPARQLResultObserver) listener).capturedObIds)
				// listerObIdListBytes += obid.getBytes().length;
				// }
				// }
				// long listenerResultListBytes = 0;
				// for (Object listener : CityBench.registeredQueries.values()) {
				//
				// if (listener instanceof CQELSResultListener) {
				// for (String result : ((CQELSResultListener) listener).capturedResults)
				// listenerResultListBytes += result.getBytes().length;
				// } else {
				// for (String result : ((CSPARQLResultObserver) listener).capturedResults)
				// listenerResultListBytes += result.getBytes().length;
				// }
				// }
				System.gc();
				Runtime rt = Runtime.getRuntime();
				double usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024.0 / 1024.0;
				// double overhead = (obMapBytes + listerObIdListBytes + listenerResultListBytes) / 1024.0 / 1024.0;
				this.memoryList.add(usedMB);
				logger.info("Current performance: L - " + currentLatency + ", Cnt: " + this.resultCntMap + ", Mem - "
						+ usedMB);// + ", monitoring overhead - " + overhead);
				Thread.sleep(5000);

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.exit(0);
	}

	private void cleanup() {
		if (CityBench.csparqlEngine != null) {
			// CityBench.csparqlEngine.destroy();
			for (Object css : CityBench.startedStreamObjects) {
				((CSPARQLSensorStream) css).stop();
			}
		} else {
			// CityBench.cqelsContext.engine().
			for (Object css : CityBench.startedStreamObjects) {
				((CQELSSensorStream) css).stop();
			}
		}
		this.stop = true;
		System.gc();

	}

	public boolean isStop() {
		return stop;
	}

	public void setStop(boolean stop) {
		this.stop = stop;
	}

	public synchronized void addResults(String qid, Map<String, Long> results, int cnt) {
		if (this.resultInitTime == 0) {
			this.resultInitTime = System.currentTimeMillis();
			this.lastCheckPoint = System.currentTimeMillis();
		}
		qid = qid.split("-")[0];
		for (Entry en : results.entrySet()) {
			String obid = en.getKey().toString();
			long delay = (long) en.getValue();
			this.latencyMap.get(qid).add(delay);
		}
		this.resultCntMap.put(qid, this.resultCntMap.get(qid) + cnt);
	}
}
