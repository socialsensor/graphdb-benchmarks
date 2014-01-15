package eu.socialsensor.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import eu.socialsensor.benchmarks.SingleInsertionBenchmark;

public class Utils {
	
	static public void main(String args[]) {
		Utils utils = new Utils();
		System.out.println(utils.getListFromTextDoc("data/test1"));
	}
	
	public void getDocumentsAs2dList(List<List<Double>> data, String docPath) {
		Utils utils = new Utils();
		for(int i = 0; i < SingleInsertionBenchmark.SCENARIOS; i++) {
			data.add(utils.getListFromTextDoc(docPath+"."+(i+1)));
		}
	}
	
	/**
	 * Calculates the mean value of x-dimenstion vector.
	 * @param data - 2d ArrayList
	 */
	public List<Double> calculateMeanList(List<List<Double>> data) {
		int yDim = data.size();
		int xDim = data.get(0).size();
		List<Double> meanData = new ArrayList<Double>();
		for(int i = 0; i < xDim; i++) {
			double[] temp = new double[yDim];
			for(int j = 0; j < yDim; j++) {
				temp[j] = data.get(j).get(i);
			}
			meanData.add(calculateMean(temp));
		}
		return meanData;
	}
	
	public double calculateMean(double[] data) {
		double sum = 0;
		double size = data.length;
        for(double a : data) {
        	sum += a;
        }
        return sum/size;
	}
		
	public double calculateVariance(double mean, double[] data) {
		double size = data.length;
		double temp = 0;
        for(double a :data) {
        	temp += (mean-a)*(mean-a);
        }
        return temp/size;
	}
		
	public double calculateStdDeviation(double var) {
		return Math.sqrt(var);
	}
	
	public void deleteRecursively(File file ) {
		if ( !file.exists() ) {
			return;
		}
		if ( file.isDirectory() ) {
			for ( File child : file.listFiles() ) {
				deleteRecursively( child );
			}
		}
		if ( !file.delete() ) {
			throw new RuntimeException( "Couldn't empty database." );
		}
	}
	
	public void deleteMultipleFiles(String filePath, int numberOfFiles) {
		for(int i = 0; i < numberOfFiles; i++) {
			deleteRecursively(new File(filePath+"."+(i+1)));
		}
	}
	
	public void writeTimes(List<Double> insertionTimes, String outputPath) {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(outputPath));
			for(Double insertionTime : insertionTimes) {
				out.write(String.valueOf(insertionTime));
				out.write("\n");
			}
			out.flush();
			out.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public List<Double> getListFromTextDoc(String docPath) {
		List<Double> values = new ArrayList<Double>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(docPath)));
			String line;
			while((line = reader.readLine()) != null) {
				values.add(Double.valueOf(line));
			}
			reader.close();
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		return values;
	}

}
