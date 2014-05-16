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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import eu.socialsensor.benchmarks.SingleInsertionBenchmark;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.main.GraphDatabaseBenchmark;

/**
 * This class contains all the required utility 
 * functions for the benchmark
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 * 
 */
public class Utils {
	
	private Logger logger = Logger.getLogger(Utils.class);
	
	static public void main(String args[]) {
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
	
	public Map<Integer, List<Integer>> mapNodesToCommunities(String dataPath) {
		Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(dataPath)));
			String line;
			while((line = reader.readLine()) != null) {
				String[] parts = line.split("\t");
				int node = Integer.valueOf(parts[0]);
				int community = Integer.valueOf(parts[1].substring(0, parts[1].length()-1)) - 1;
				if(!communities.containsKey(community)) {
					List<Integer> nodes = new ArrayList<Integer>();
					nodes.add(node);
					communities.put(community, nodes);
				}
				else {
					communities.get(community).add(node);
				}
			}
			reader.close();
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return communities;
	}
	
//	public long getKeyByValue(Map<Long, Integer> map, int value) {
//		long key = 0;
//		for(Map.Entry<Long, Integer> entry : map.entrySet()) {
//			if(value == entry.getValue()) {
//				key = entry.getKey();
//				break;
//			}
//		}
//		return key;
//	}
	
	public <T, E> T getKeyByValue(Map<T, E> map, E value) {
		for (Entry<T, E> entry : map.entrySet()) {
	        if (value.equals(entry.getValue())) {
	            return entry.getKey();
	        }
	    }
	    return null;
	}

	public void createDatabases(String dataset) {
		logger.info("Creating graph databases before benchmark execution . . . .");
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		titanGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.TITANDB_PATH);
		titanGraphDatabase.massiveModeLoading(dataset);
		titanGraphDatabase.shutdownMassiveGraph();
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		orientGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.ORIENTDB_PATH);
		orientGraphDatabase.massiveModeLoading(dataset);
		orientGraphDatabase.shutdownMassiveGraph();
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.NEO4JDB_PATH);
		neo4jGraphDatabase.massiveModeLoading(dataset);
		neo4jGraphDatabase.shutdownMassiveGraph();
	}
	
	public void deleteDatabases() {
		logger.info("Deleting graph databese . . . .");
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		titanGraphDatabase.delete(GraphDatabaseBenchmark.TITANDB_PATH);
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		orientGraphDatabase.delete(GraphDatabaseBenchmark.ORIENTDB_PATH);
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.delete(GraphDatabaseBenchmark.NEO4JDB_PATH);
	}
}
