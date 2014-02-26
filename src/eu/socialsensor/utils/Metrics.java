package eu.socialsensor.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class implements the metrics we use for the evaluation
 * of the predicted clustering. For now we use only the NMI
 * 
 * @author sbeis
 * @email sot.beis@gmail.com
 *
 */
public class Metrics {
	
	public static void main(String args[]) {
		Metrics metrics = new Metrics();
		Map<Integer, List<Integer>> map1 = new HashMap<Integer, List<Integer>>();
		List<Integer> list1 = new ArrayList<Integer>();
		list1.add(1);
		list1.add(2);
		list1.add(3);
		list1.add(4);
		List<Integer> list2 = new ArrayList<Integer>();
		list2.add(5);
		list2.add(6);
		list2.add(7);
		list2.add(8);
		map1.put(1, list1);
		map1.put(2, list2);
		
		
		Map<Integer, List<Integer>> map2 = new HashMap<Integer, List<Integer>>();
		List<Integer> list3 = new ArrayList<Integer>();
		list3.add(5);
		list3.add(2);
		list3.add(3);
		list3.add(4);
		List<Integer> list4 = new ArrayList<Integer>();
		list4.add(1);
		list4.add(6);
		list4.add(7);
		list4.add(8);
		map2.put(1, list3);
		map2.put(2, list4);
		
		System.out.println(metrics.normalizedMutualInformation(8, map1, map2));
	}
	
	public double normalizedMutualInformation(int numberOfNodes, Map<Integer, List<Integer>> actualPartitions, 
			Map<Integer, List<Integer>> predictedPartitions) {
		double nmi = 0;
		double numOfNodes = (double)numberOfNodes;
		int[][] confusionMatrix = confusionMatrix(actualPartitions, predictedPartitions);
		int[] confusionMatrixActual = new int[actualPartitions.size()];
		int[] confusionMatrixPredicted = new int[predictedPartitions.size()];
		for(int i = 0; i < confusionMatrixActual.length; i++) {
			int sum = 0;
			for(int j = 0; j < confusionMatrixPredicted.length; j++) {
				sum = sum + confusionMatrix[i][j];
			}
			confusionMatrixActual[i] = sum;
		}
		for(int j = 0; j < confusionMatrixPredicted.length; j++) {
			int sum = 0;
			for(int i = 0; i < confusionMatrixActual.length; i++) {
				sum = sum + confusionMatrix[i][j];
			}
			confusionMatrixPredicted[j] = sum;
		}
		
		double term1 = 0;
		for(int i = 0; i < confusionMatrixActual.length; i++) {
			for(int j = 0; j < confusionMatrixPredicted.length; j++) {
				if(confusionMatrix[i][j] > 0) {
					term1 += -2.0 * confusionMatrix[i][j] * 
							Math.log((confusionMatrix[i][j] * numOfNodes) / 
									(confusionMatrixActual[i] * confusionMatrixPredicted[j]));
				}
			}
		}
		double term2 = 0;
		for(int i = 0; i < confusionMatrixActual.length; i++) {
			term2 += confusionMatrixActual[i] * Math.log(confusionMatrixActual[i] / numOfNodes);
		}
		double term3 = 0;
		for(int j = 0; j < confusionMatrixPredicted.length; j++) {
			term3 += confusionMatrixPredicted[j] * Math.log(confusionMatrixPredicted[j] / numOfNodes);
		}
		nmi = term1 / (term2 + term3);
		return nmi;
	}
	
	private int[][] confusionMatrix(Map<Integer, List<Integer>> actualPartitions, 
			Map<Integer, List<Integer>> predictedPartitions) {
		int actualPartitionsSize = actualPartitions.size();
		int predictedPartitionsSize = predictedPartitions.size();
		int [][] confusionMatrix = new int[actualPartitionsSize][];
		int actualPartitionsKeys[] = new int[actualPartitionsSize];
		int predictedPartitionsKeys[] = new int[predictedPartitionsSize];
		
		int actualPartitionsIndex = 0;
		for(int key : actualPartitions.keySet()) {
			actualPartitionsKeys[actualPartitionsIndex] = key;
			actualPartitionsIndex++;
		}
		int predictedPartitionsIndex = 0;
		for(int key : predictedPartitions.keySet()) {
			predictedPartitionsKeys[predictedPartitionsIndex] = key;
			predictedPartitionsIndex++;
		}
		
		for(int i = 0; i < actualPartitionsSize; i++) {
			confusionMatrix[i] = new int[predictedPartitionsSize];
			for(int j = 0; j < predictedPartitionsSize; j++) {
				int commonNodes = 0;
				for(int node : predictedPartitions.get(predictedPartitionsKeys[j])) {
					if(actualPartitions.get(actualPartitionsKeys[i]).contains(node)) {
						commonNodes++;
					}
				}
				confusionMatrix[i][j] = commonNodes;
			}
		}
		return confusionMatrix;
	}
}
