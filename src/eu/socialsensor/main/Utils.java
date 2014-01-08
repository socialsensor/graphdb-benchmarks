package eu.socialsensor.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Utils {
	
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

}
