package eu.socialsensor.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Utils {
	
	public static void main(String args[]) {
		Utils utils = new Utils();
		List<Double> list1 = new ArrayList<Double>();
		list1.add(1.0);
		list1.add(2.0);
		list1.add(3.0);
		list1.add(3.0);
		List<Double> list2 = new ArrayList<Double>();
		list2.add(4.0);
		list2.add(5.0);
		list2.add(6.0);
		list2.add(6.0);
		List<Double> list3 = new ArrayList<Double>();
		list3.add(7.0);
		list3.add(8.0);
		list3.add(9.0);
		list3.add(9.0);
		List<List<Double>> data = new ArrayList<List<Double>>();
		data.add(list1);
		data.add(list2);
		data.add(list3);
		utils.calculateMeanList(data);
	}
	
	/**
	 * Calculates the mean value of x-dimenstion vector.
	 * @param data - 2d ArrayList
	 */
	public List<Double> calculateMeanList(List<List<Double>> data) {
		int yDim = data.size();
		int xDim = data.get(0).size();
		List<Double> meanData = new ArrayList<Double>();
		for(int i = 0; i < yDim; i++) {
			double[] temp = new double[xDim];
			for(int j = 0; j < xDim; j++) {
				temp[j] = data.get(i).get(j);
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
