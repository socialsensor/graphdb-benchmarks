package eu.socialsensor.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class Utils {
	
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
	
	public void writeTimes(List<Long> insertionTimes, String outputPath) {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(outputPath));
			for(Long insertionTime : insertionTimes) {
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
