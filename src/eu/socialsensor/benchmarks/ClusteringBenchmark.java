package eu.socialsensor.benchmarks;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import eu.socialsensor.clustering.LouvainMethod;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.main.GraphDatabaseBenchmark;

/**
 * ClusteringBenchmark implementation
 * @author sotbeis
 * @email sotbeis@iti.gr
 */
public class ClusteringBenchmark implements Benchmark {
	
	private final List<Integer> cacheSizes;
	public final static boolean IS_RANDOMIZED = false;
	public static final String SYNTH_DATASET = "SynthGraph500";
	
	private final String resultFile = "CWResults.txt";
	
	BufferedWriter out;
	
	private Logger logger = Logger.getLogger(ClusteringBenchmark.class);
	
	public ClusteringBenchmark() {
		String cacheProperty = GraphDatabaseBenchmark.inputPropertiesFile.getProperty("CACHE_VALUES");
		String cacheProStringParts[] = cacheProperty.split(",");
		cacheSizes = new ArrayList<Integer>(cacheProStringParts.length);
		for(int i = 0; i < cacheProStringParts.length; i++) {
			cacheSizes.add(Integer.valueOf(cacheProStringParts[i]));
		}
	}
	
	@Override
	public void startBenchmark() {
		
		logger.setLevel(Level.INFO);
		logger.info("Executing Clustering Benchmark . . . .");
		String resultsFolder = GraphDatabaseBenchmark.inputPropertiesFile.getProperty("RESULTS_PATH");
		String output = resultsFolder+resultFile;
		try {
			out = new BufferedWriter(new FileWriter(output));
			out.write("###########################################################");
			out.write("\n");
			out.write("##### Clustering Benchmark with "+ SYNTH_DATASET +" Results #####");
			out.write("\n");
			out.write("###########################################################");
			out.write("\n");
			out.write("\n");
			out.write("Neo4j execution time");
			out.write("\n");
			neo4jClusteringBenchmark(GraphDatabaseBenchmark.NEO4JDB_PATH);
			out.write("\n");
			out.write("\n");
			out.write("OrientDB execution time");
			out.write("\n");
			orientClusteringBenchmark(GraphDatabaseBenchmark.ORIENTDB_PATH);
			out.write("\n");
			out.write("\n");
			out.write("Titan execution time");
			out.write("\n");
			titanClusteringBenchmark(GraphDatabaseBenchmark.TITANDB_PATH);
			out.flush();
			out.close();
		} 
		catch (ExecutionException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		logger.info("Clustering Benchmark finished");
		
	}
	
	private void titanClusteringBenchmark(String dbPAth) throws ExecutionException, IOException {
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		titanGraphDatabase.open(dbPAth);
		int runs = 1;
		int numberOfLoops = 2;
		out.write("Cache Size,Time(sec)");
		out.write("\n");
		for(int i = 0; i < cacheSizes.size(); i++) {
			if(runs > 1) {
				numberOfLoops = 1;
			}
			for(int j = 0; j < numberOfLoops; j++) {
				int cacheSize = cacheSizes.get(i);
				logger.info("Graph Database = Titat, Dataset = "+SYNTH_DATASET+", Cache Size = "+cacheSize);				long start = System.currentTimeMillis();
				LouvainMethod louvainMethodCache = new LouvainMethod(titanGraphDatabase, cacheSize, IS_RANDOMIZED);
				louvainMethodCache.computeModularity();
				double titanTime = (System.currentTimeMillis() - start) / 1000.0;
				out.write(cacheSize + ","+titanTime);
				out.write("\n");
			}
			runs++;
		}
		titanGraphDatabase.shutdown();
	}
	
	private void orientClusteringBenchmark(String dbPAth) throws ExecutionException, IOException {
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		orientGraphDatabase.open(dbPAth);
		int runs = 1;
		int numberOfLoops = 2;
		out.write("Cache Size,Time(sec)");
		out.write("\n");
		for(int i = 0; i < cacheSizes.size(); i++) {
			if(runs > 1) {
				numberOfLoops = 1;
			}
			for(int j = 0; j < numberOfLoops; j++) {
				long start = System.currentTimeMillis();
				int cacheSize = cacheSizes.get(i);
				logger.info("Graph Database = OrientDB, Dataset = "+SYNTH_DATASET+", Cache Size = "+cacheSize);
				LouvainMethod louvainMethodCache = new LouvainMethod(orientGraphDatabase, cacheSize, IS_RANDOMIZED);
				louvainMethodCache.computeModularity();
				double orientTime = (System.currentTimeMillis() - start) / 1000.0;
				out.write(cacheSize + ","+orientTime);
				out.write("\n");
			}
			runs++;
		}
		orientGraphDatabase.shutdown();
	}
	
	private void neo4jClusteringBenchmark(String dbPAth) throws ExecutionException, IOException {
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.open(dbPAth);
		int runs = 1;
		int numberOfLoops = 2;
		out.write("Cache Size,Time(sec)");
		out.write("\n");
		for(int i = 0; i < cacheSizes.size(); i++) {
			if(runs > 1) {
				numberOfLoops = 1;
			}
			for(int j = 0; j < numberOfLoops; j ++) {
				long start = System.currentTimeMillis();
				int cacheSize = cacheSizes.get(i);
				logger.info("Graph Database = Neo4j, Dataset = "+SYNTH_DATASET+", Cache Size = "+cacheSize);				LouvainMethod louvainMethodCache = new LouvainMethod(neo4jGraphDatabase, cacheSize, IS_RANDOMIZED);
				louvainMethodCache.computeModularity();
				double neo4jTime = (System.currentTimeMillis() - start) / 1000.0;
				out.write(cacheSize + ","+neo4jTime);
				out.write("\n");
			}
			runs++;
		}
		neo4jGraphDatabase.shutdown();
	}
	
}
