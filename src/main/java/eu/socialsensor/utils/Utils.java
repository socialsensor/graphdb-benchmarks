package eu.socialsensor.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.SparkseeGraphDatabase;
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
		Utils utils = new Utils();
		System.out.println(utils.calculateFactorial(6));
	}
	
	public void getDocumentsAs2dList(List<List<Double>> data, String docPath) {
		Utils utils = new Utils();
		for(int i = 0; i < GraphDatabaseBenchmark.SCENARIOS; i++) {
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
		
	public <T, E> T getKeyByValue(Map<T, E> map, E value) {
		for (Entry<T, E> entry : map.entrySet()) {
	        if (value.equals(entry.getValue())) {
	            return entry.getKey();
	        }
	    }
	    return null;
	}
	
	public int calculateFactorial(int n) {
		if(n < 0) {
			throw new Error("Number should be non-negative.");
		}
		else if(n == 0) {
			return 1;
		}
		else {
			int factorial = 1;
			for (int i = 1 ; i <= n ; i++ ) {
				factorial = factorial * i;
			}
			return factorial;
		}
	}
	
	public void clearGC() {
		Object obj = new Object();
	    WeakReference<Object> ref = new WeakReference<Object>(obj);
	    obj = null;
	    while(ref.get() != null) {
	    	System.gc();
	    }
	}

	public void createDatabases(String dataset) {
		System.out.println("");
		logger.setLevel(Level.INFO);
		logger.info("Creating graph databases before benchmark execution . . . .");
		
		if(GraphDatabaseBenchmark.TITAN_SELECTED) {
			GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
			titanGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.TITANDB_PATH);
			titanGraphDatabase.massiveModeLoading(dataset);
			titanGraphDatabase.shutdownMassiveGraph();
			clearGC();
		}
				
		if(GraphDatabaseBenchmark.ORIENTDB_SELECTED) {
			GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
			orientGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.ORIENTDB_PATH);
			orientGraphDatabase.massiveModeLoading(dataset);
			orientGraphDatabase.shutdownMassiveGraph();
			clearGC();
		}
		
		if(GraphDatabaseBenchmark.NEO4J_SELECTED) {
			GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
			neo4jGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.NEO4JDB_PATH);
			neo4jGraphDatabase.massiveModeLoading(dataset);
			neo4jGraphDatabase.shutdownMassiveGraph();
			clearGC();
		}
		
		if(GraphDatabaseBenchmark.SPARKSEE_SELECTED) {
			GraphDatabase sparkseeGraphDatabase = new SparkseeGraphDatabase();
			sparkseeGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.SPARKSEEDB_PATH);
			sparkseeGraphDatabase.massiveModeLoading(dataset);
			sparkseeGraphDatabase.shutdownMassiveGraph();
			clearGC();
		}
	}
	
	public void deleteDatabases() {
		System.out.println("");
		logger.setLevel(Level.INFO);
		logger.info("Deleting graph databases . . . .");
		
		if(GraphDatabaseBenchmark.TITAN_SELECTED) {
			GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
			titanGraphDatabase.delete(GraphDatabaseBenchmark.TITANDB_PATH);
		}
		if(GraphDatabaseBenchmark.ORIENTDB_SELECTED) {
			GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
			orientGraphDatabase.delete(GraphDatabaseBenchmark.ORIENTDB_PATH);
		}
		if(GraphDatabaseBenchmark.NEO4J_SELECTED) {
			GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
			neo4jGraphDatabase.delete(GraphDatabaseBenchmark.NEO4JDB_PATH);
		}
		if(GraphDatabaseBenchmark.SPARKSEE_SELECTED) {
			GraphDatabase sparkseeGraphDatabase = new SparkseeGraphDatabase();
			sparkseeGraphDatabase.delete(GraphDatabaseBenchmark.SPARKSEEDB_PATH);
		}
	}
	
	public Method[] filter(Method[] declaredMethods, String endsWith) {
	    List<Method> filtered = new ArrayList<>();
	    for(Method method : declaredMethods) {
	    	if(method.getName().endsWith(endsWith)) {
	    		if(GraphDatabaseBenchmark.TITAN_SELECTED && 
	    				method.getName().contains(GraphDatabaseBenchmark.TITAN)) {
	    			filtered.add(method);
	    		}
	    		if(GraphDatabaseBenchmark.ORIENTDB_SELECTED &&
	    				method.getName().contains(GraphDatabaseBenchmark.ORIENTDB)) {
	    			filtered.add(method);
	    		}
	    		if(GraphDatabaseBenchmark.NEO4J_SELECTED &&
	    				method.getName().contains(GraphDatabaseBenchmark.NEO4J)) {
	    			filtered.add(method);
	    		}
	    		if(GraphDatabaseBenchmark.SPARKSEE_SELECTED &&
	    				method.getName().contains(GraphDatabaseBenchmark.SPARKSEE)) {
	    			filtered.add(method);
	    		}
	    	}
	    }
	    return filtered.toArray(new Method[filtered.size()]);
	}
	
	public void selectDatabases(String selectedDatabases) {
		String[] dbs = selectedDatabases.split(",");
		for(String db : dbs) {
			if(db.equals(GraphDatabaseBenchmark.TITAN)) {
				GraphDatabaseBenchmark.TITAN_SELECTED = true;
			}
			if(db.equals(GraphDatabaseBenchmark.ORIENTDB)) {
				GraphDatabaseBenchmark.ORIENTDB_SELECTED = true;
			}
			if(db.equals(GraphDatabaseBenchmark.NEO4J)) {
				GraphDatabaseBenchmark.NEO4J_SELECTED = true;
			}
			if(db.equals(GraphDatabaseBenchmark.SPARKSEE)) {
				GraphDatabaseBenchmark.SPARKSEE_SELECTED = true;
			}
		}
	}
	
	public void writeResults(double[] titanTimes, double[] orientTimes, double[] neo4jTimes, 
			double[] sparkseeTimes, String resultsFile, String benchmarkTitle) {
		
		String meanTitanTimeString;
		String stdTitanTimeString;
		if(GraphDatabaseBenchmark.TITAN_SELECTED) {
			double meanTitanTime = calculateMean(titanTimes);
			double varTitanTime = calculateVariance(meanTitanTime, titanTimes);
			double stdTitanTime = calculateStdDeviation(varTitanTime);
			meanTitanTimeString = String.valueOf(meanTitanTime);
			stdTitanTimeString = String.valueOf(stdTitanTime);
		}
		else {
			meanTitanTimeString = "Titan was not selected";
			stdTitanTimeString = "Titan was not selected";
		}
		
		String meanOrientTimeString;
		String stdOrientTimeString;
		if(GraphDatabaseBenchmark.ORIENTDB_SELECTED) {
			double meanOrientTime = calculateMean(orientTimes);
			double varOrientTime = calculateVariance(meanOrientTime, orientTimes);
			double stdOrientTime = calculateStdDeviation(varOrientTime);
			meanOrientTimeString = String.valueOf(meanOrientTime);
			stdOrientTimeString = String.valueOf(stdOrientTime);
		}
		else {
			meanOrientTimeString = "OrientDB was not selected";
			stdOrientTimeString = "OrientDB was not selected";
		}
		
		String meanNeo4jTimeString;
		String stdNeo4jTimeString;
		if(GraphDatabaseBenchmark.NEO4J_SELECTED) {
			double meanNeo4jTime = calculateMean(neo4jTimes);
			double varNeo4jTime = calculateVariance(meanNeo4jTime, neo4jTimes);
			double stdNeo4jTime = calculateStdDeviation(varNeo4jTime);
			meanNeo4jTimeString = String.valueOf(meanNeo4jTime);
			stdNeo4jTimeString = String.valueOf(stdNeo4jTime);
		}
		else {
			meanNeo4jTimeString = "Neo4j was not selected";
			stdNeo4jTimeString = "Neo4j was not selected";
		}
		
		String meanSparkseeTimeString;
		String stdSparkseeTimeString;
		if(GraphDatabaseBenchmark.SPARKSEE_SELECTED) {
			double meanSparkseeTime = calculateMean(sparkseeTimes);
			double varSparkseeTime = calculateVariance(meanSparkseeTime, sparkseeTimes);
			double stdSparkseeTime = calculateStdDeviation(varSparkseeTime);
			meanSparkseeTimeString = String.valueOf(meanSparkseeTime);
			stdSparkseeTimeString = String.valueOf(stdSparkseeTime);
		}
		else {
			meanSparkseeTimeString = "Sparksee was not selected";
			stdSparkseeTimeString = "Sparksee was not selected";
		}

		String output = GraphDatabaseBenchmark.RESULTS_PATH + resultsFile;
		logger.setLevel(Level.INFO);
		System.out.println("");
		logger.info("Write results to "+output);
		String title = benchmarkTitle + "Benchmark Results";
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(output));
			out.write("##############################################################");
			out.write("\n");
			out.write("######### " + title + " #########");
			out.write("\n");
			out.write("##############################################################");
			out.write("\n");
			out.write("\n");
			out.write("OrientDB execution time");
			out.write("\n");
			out.write("Mean Value: "+meanOrientTimeString);
			out.write("\n");
			out.write("STD Value: "+stdOrientTimeString);
			out.write("\n");
			out.write("\n");
			out.write("Titan execution time");
			out.write("\n");
			out.write("Mean Value: "+meanTitanTimeString);
			out.write("\n");
			out.write("STD Value: "+stdTitanTimeString);
			out.write("\n");
			out.write("\n");
			out.write("Neo4j execution time");
			out.write("\n");
			out.write("Mean Value: "+meanNeo4jTimeString);
			out.write("\n");
			out.write("STD Value: "+stdNeo4jTimeString);
			out.write("\n");
			out.write("\n");
			out.write("Sparksee execution time");
			out.write("\n");
			out.write("Mean Value: " + meanSparkseeTimeString);
			out.write("\n");
			out.write("STD Value: " + stdSparkseeTimeString);
			out.write("\n");
			out.write("########################################################");
			
			out.flush();
			out.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
