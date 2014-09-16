package eu.socialsensor.benchmarks;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import eu.socialsensor.clustering.LouvainMethod;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.SparkseeGraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.main.GraphDatabaseBenchmark;
import eu.socialsensor.utils.Metrics;
import eu.socialsensor.utils.Utils;

/**
 * ClusteringBenchmark implementation
 * @author sotbeis
 * @email sotbeis@iti.gr
 */
public class ClusteringBenchmark implements Benchmark {
	
	private final List<Integer> cacheSizes;
	public final static boolean IS_RANDOMIZED = false;
	public static String SYNTH_DATASET;
	
	private static final String CW_RESULTS = "CWResults.txt";
	
	private int nodesCnt;
	
	BufferedWriter out;
	
	private Logger logger = Logger.getLogger(ClusteringBenchmark.class);
	
	public ClusteringBenchmark() {
		String cacheProperty = GraphDatabaseBenchmark.inputPropertiesFile.getProperty("CACHE_VALUES");
		this.nodesCnt = Integer.valueOf(GraphDatabaseBenchmark.inputPropertiesFile
				.getProperty("NODES_CNT"));
		int cacheValuesCnt = Integer.valueOf(GraphDatabaseBenchmark.inputPropertiesFile.
				getProperty("CACHE_VALUES_CNT"));
		double cacheIncrementFactor = Double.valueOf(GraphDatabaseBenchmark.inputPropertiesFile.
				getProperty("CACHE_INCREMENT_FACTOR"));
		cacheSizes = new ArrayList<Integer>(cacheValuesCnt);
		
		if(!cacheProperty.isEmpty()) {
			String cacheProStringParts[] = cacheProperty.split(",");
			for(int i = 0; i < cacheProStringParts.length; i++) {
				cacheSizes.add(Integer.valueOf(cacheProStringParts[i]));
			}
		}
		else {
			for(int i = 0; i < cacheValuesCnt; i ++) {
				double cacheSize = nodesCnt * cacheIncrementFactor;
				cacheSizes.add((int)cacheSize);
				cacheIncrementFactor += cacheIncrementFactor;
			}
		}
		SYNTH_DATASET = "SynthGraph" + nodesCnt;
	}
	
	@Override
	public void startBenchmark() {
		
		logger.setLevel(Level.INFO);
		logger.info("Executing Clustering Benchmark . . . .");
		String output = GraphDatabaseBenchmark.RESULTS_PATH + ClusteringBenchmark.CW_RESULTS;
		try {
			out = new BufferedWriter(new FileWriter(output));
			out.write("###########################################################");
			out.write("\n");
			out.write("##### Clustering Benchmark with "+ SYNTH_DATASET +" Results #####");
			out.write("\n");
			out.write("###########################################################");
			
			if(GraphDatabaseBenchmark.NEO4J_SELECTED) {
				out.write("\n");
				out.write("\n");
				out.write("Neo4j execution time");
				out.write("\n");
				neo4jClusteringBenchmark(GraphDatabaseBenchmark.NEO4JDB_PATH);
			}
			
			if(GraphDatabaseBenchmark.ORIENTDB_SELECTED) {
				out.write("\n");
				out.write("\n");
				out.write("OrientDB execution time");
				out.write("\n");
				orientClusteringBenchmark(GraphDatabaseBenchmark.ORIENTDB_PATH);
			}
			
			if(GraphDatabaseBenchmark.TITAN_SELECTED) {
				out.write("\n");
				out.write("\n");
				out.write("Titan execution time");
				out.write("\n");
				titanClusteringBenchmark(GraphDatabaseBenchmark.TITANDB_PATH);
			}
			
			if(GraphDatabaseBenchmark.SPARKSEE_SELECTED) {
				out.write("\n");
				out.write("\n");
				out.write("Sparksee execution time");
				out.write("\n");
				sparkseeClusteringBenchmark(GraphDatabaseBenchmark.SPARKSEEDB_PATH);

			}
			
			out.write("\n");
			out.write("###########################################################");
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
		titanGraphDatabase.setClusteringWorkload(true);
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
				logger.info("Graph Database: Titan, Dataset: " + SYNTH_DATASET + ", Cache Size: " + cacheSize);				
				long start = System.currentTimeMillis();
				LouvainMethod louvainMethodCache = new LouvainMethod(titanGraphDatabase, cacheSize, IS_RANDOMIZED);
				louvainMethodCache.computeModularity();
				double titanTime = (System.currentTimeMillis() - start) / 1000.0;
				out.write(cacheSize + ","+titanTime);
				out.write("\n");
				
				//evaluation with NMI
				Map<Integer, List<Integer>> predictedCommunities = titanGraphDatabase.mapCommunities(louvainMethodCache.getN());
				Utils utils = new Utils();
				Map<Integer, List<Integer>> actualCommunities = utils.mapNodesToCommunities("./data/community.dat");
				Metrics metrics = new Metrics();
				double NMI = metrics.normalizedMutualInformation(this.nodesCnt, actualCommunities, predictedCommunities);
				logger.info("NMI value: " + NMI);
			}
			runs++;
		}
		titanGraphDatabase.shutdown();
	}
	
	private void orientClusteringBenchmark(String dbPAth) throws ExecutionException, IOException {
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		orientGraphDatabase.setClusteringWorkload(true);
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
				logger.info("Graph Database: OrientDB, Dataset: " + SYNTH_DATASET+", Cache Size: " + cacheSize);
				LouvainMethod louvainMethodCache = new LouvainMethod(orientGraphDatabase, cacheSize, IS_RANDOMIZED);
				louvainMethodCache.computeModularity();
				double orientTime = (System.currentTimeMillis() - start) / 1000.0;
				out.write(cacheSize + ","+orientTime);
				out.write("\n");
				
				//evaluation with NMI
				Map<Integer, List<Integer>> predictedCommunities = orientGraphDatabase.mapCommunities(louvainMethodCache.getN());
				Utils utils = new Utils();
				Map<Integer, List<Integer>> actualCommunities = utils.mapNodesToCommunities("./data/community.dat");
				Metrics metrics = new Metrics();
				double NMI = metrics.normalizedMutualInformation(this.nodesCnt, actualCommunities, predictedCommunities);
				logger.info("NMI value: " + NMI);
			}
			runs++;
		}
		orientGraphDatabase.shutdown();
	}
	
	private void neo4jClusteringBenchmark(String dbPAth) throws ExecutionException, IOException {
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.setClusteringWorkload(true);
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
				logger.info("Graph Database: Neo4j, Dataset: " + SYNTH_DATASET + ", Cache Size: " + cacheSize);				
				LouvainMethod louvainMethodCache = new LouvainMethod(neo4jGraphDatabase, cacheSize, IS_RANDOMIZED);
				louvainMethodCache.computeModularity();
				double neo4jTime = (System.currentTimeMillis() - start) / 1000.0;
				out.write(cacheSize + ","+neo4jTime);
				out.write("\n");
				
				//evaluation with NMI
				Map<Integer, List<Integer>> predictedCommunities = neo4jGraphDatabase.mapCommunities(louvainMethodCache.getN());
				Utils utils = new Utils();
				Map<Integer, List<Integer>> actualCommunities = utils.mapNodesToCommunities("./data/community.dat");
				Metrics metrics = new Metrics();
				double NMI = metrics.normalizedMutualInformation(this.nodesCnt, actualCommunities, predictedCommunities);
				logger.info("NMI value: " + NMI);
			}
			runs++;
		}
		neo4jGraphDatabase.shutdown();
	}
	
	private void sparkseeClusteringBenchmark(String dbPath) throws IOException, ExecutionException {
		GraphDatabase sparkseeGraphDatabase = new SparkseeGraphDatabase();
		sparkseeGraphDatabase.setClusteringWorkload(true);
		sparkseeGraphDatabase.open(dbPath);
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
				logger.info("Graph Database: Sparksee, Dataset: " + SYNTH_DATASET + ", Cache Size: " + cacheSize);				
				LouvainMethod louvainMethodCache = new LouvainMethod(sparkseeGraphDatabase, cacheSize, IS_RANDOMIZED);
				louvainMethodCache.computeModularity();
				double sparkseeTime = (System.currentTimeMillis() - start) / 1000.0;
				out.write(cacheSize + "," + sparkseeTime);
				out.write("\n");
				
				//evaluation with NMI
				Map<Integer, List<Integer>> predictedCommunities = sparkseeGraphDatabase.mapCommunities(louvainMethodCache.getN());
				Utils utils = new Utils();
				Map<Integer, List<Integer>> actualCommunities = utils.mapNodesToCommunities("./data/community.dat");
				Metrics metrics = new Metrics();
				double NMI = metrics.normalizedMutualInformation(this.nodesCnt, actualCommunities, predictedCommunities);
				logger.info("NMI value: " + NMI);
			}
			runs++;
		}
		sparkseeGraphDatabase.shutdown();
	}
	
}