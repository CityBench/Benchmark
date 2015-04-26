package org.insight_centre.aceis.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author feng
 * 
 *         creates different partitions for the direct sub-trees of an event pattern
 */
public class Partitioner {
	// ArrayList resultSet;
	// ArrayList initialSet;

	private static String convertBinary(int sum) {
		StringBuffer binary = new StringBuffer();
		while (sum != 0 && sum != 1) {
			binary.insert(0, sum % 2);
			// println("sum=" + sum + "ำเสý=" + (sum % 2) + "ณýสý=" + sum / 2);
			sum = sum / 2;
			if (sum == 0 || sum == 1) {
				binary.insert(0, sum % 2);
			}
		}
		return binary.toString();
	}

	public static List<Integer> getFactors(int card) {
		List<Integer> results = new ArrayList<Integer>();
		results.add(card);
		for (int i = 2; i <= card / 2; i++) {
			if (card % i == 0)
				results.add(card / i);
		}
		return results;
	}

	@SuppressWarnings("rawtypes")
	public static ArrayList getOrderedPartition(ArrayList initialSet) {
		ArrayList resultSet = new ArrayList();
		List<String> codeList = new ArrayList<String>();
		for (int i = 1; i < Math.pow(2, initialSet.size() - 1); i++) {
			String code = "";
			if (i == 1)
				code = "1";
			else
				code = convertBinary(i);
			int appendSize = initialSet.size() - code.length() - 1;
			for (int j = 0; j < appendSize; j++) {
				code = "0" + code;
			}
			codeList.add(code);
		}
		for (String code : codeList) {
			if (code.contains("0")) // code in the form of "111...1" is discarded
				resultSet.add(splitSequenceWithCode(code, initialSet));
		}
		return resultSet;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static ArrayList getUnorderedPartition(ArrayList initialSet) {
		ArrayList resultSet = new ArrayList();
		ArrayList onePartition = new ArrayList();
		ArrayList tempSet = new ArrayList();
		ArrayList tempResult = new ArrayList();
		tempSet.add(initialSet.get(0));
		onePartition.add(tempSet);
		tempResult.add(onePartition);
		for (int i = 1; i < initialSet.size(); i++) {
			ArrayList tempSet1 = new ArrayList();
			tempSet1.add(initialSet.get(i));
			for (int j = 0; j < tempResult.size(); j++) {
				// onePartition.clear();
				onePartition = (ArrayList) tempResult.get(j);
				ArrayList aNewPartition = new ArrayList();
				aNewPartition.add(tempSet1);
				for (int k = 0; k < onePartition.size(); k++) {
					aNewPartition.add(onePartition.get(k));
				}
				resultSet.add(aNewPartition);
				for (int m = 0; m < onePartition.size(); m++) {
					ArrayList bNewPartition = new ArrayList();
					for (int n = 0; n < onePartition.size(); n++) {
						bNewPartition.add(onePartition.get(n));
					}

					ArrayList tempSet3 = new ArrayList();
					ArrayList tempSet2 = new ArrayList();
					tempSet3 = (ArrayList) bNewPartition.get(m);
					for (int p = 0; p < tempSet3.size(); p++) {
						tempSet2.add(tempSet3.get(p));
					}
					tempSet2.add(initialSet.get(i));
					bNewPartition.set(m, tempSet2);
					resultSet.add(bNewPartition);
				}

			}
			ArrayList result = new ArrayList();
			for (int q = 0; q < resultSet.size(); q++) {
				result.add(resultSet.get(q));
			}
			tempResult = result;
			resultSet.clear();
		}
		resultSet = tempResult;
		ArrayList resultToRemove = new ArrayList();

		for (Object result : resultSet) {
			if (((ArrayList) result).size() == initialSet.size() || ((ArrayList) result).size() == 1) {
				resultToRemove.add(result);
				// break;
			}
		}
		resultSet.removeAll(resultToRemove);
		return resultSet;
	}

	// public Partitioner(ArrayList initialSet) {
	// this.initialSet = initialSet;
	// }
	// @SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) {

		ArrayList test = new ArrayList();
		for (int i = 0; i < 1; i++) {
			test.add(i + "");
		}
		// // Partitioner par = new Partitioner(test);
		ArrayList result = Partitioner.getOrderedPartition(test);
		for (int i = 0; i < result.size(); i++) {
			System.out.println(result.get(i));
		}
		System.out.println("\n The number of different partitions: " + result.size() + " !");
		// System.out.println(Partitioner.convertBinary(4));
	}

	private static Object splitSequenceWithCode(String code, ArrayList initialSet) {
		ArrayList resultSet = new ArrayList();
		ArrayList temp = new ArrayList();
		for (int i = 0; i < code.length(); i++) {
			temp.add(initialSet.get(i));
			if ((code.charAt(i) + "").equals("1")) {
				resultSet.add(temp.clone());
				temp = new ArrayList();
			}

		}
		temp.add(initialSet.get(initialSet.size() - 1));
		// if (temp.size() > 0)
		resultSet.add(temp);
		return resultSet;
	}
}
