package eu.socialsensor.benchmarks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import eu.socialsensor.clustering.LouvainMethodCache;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.main.GraphDatabaseBenchmark;


public class ClusteringBenchmark {
	
	private final List<Integer> cacheSizes;
		
	public final static boolean IS_RANDOMIZED = false;
	
	public static final String SYNTH_DATASET = "SynthGraph500";
	
	public ClusteringBenchmark() {
		
		cacheSizes = new ArrayList<Integer>(
				Arrays.asList(25, 50, 75, 100, 125, 150));
		
	}
	
	public void startBenchmark() throws ExecutionException {
		
		System.out.println("###########################################################");
		System.out.println("############## Starting Clustering Benchmark ##############");
		System.out.println("###########################################################");
		
		for(int i = 0; i < cacheSizes.size(); i++) {
			System.out.println("Cache size is set to "+cacheSizes.get(i));
			System.out.println("GRAPHDATABASE: Neo4j");
			System.out.println("DATASET:"+SYNTH_DATASET);
			System.out.println("CACHE: "+cacheSizes.get(i));
			neo4jClusteringBenchmark(cacheSizes.get(i), GraphDatabaseBenchmark.NEO4J_SYNTHETIC_GRAPH);
			System.out.println("=======================================================");
			System.out.println();
			System.out.println();
		}
		
		for(int i = 0; i < cacheSizes.size(); i++) {
			System.out.println("Cache size is set to "+cacheSizes.get(i));
			System.out.println("GRAPHDATABASE: OrientDB");
			System.out.println("DATASET:"+SYNTH_DATASET);
			System.out.println("CACHE: "+cacheSizes.get(i));
			orientClusteringBenchmark(cacheSizes.get(i), GraphDatabaseBenchmark.ORIENT_SYNTHETIC_GRAPH);
			System.out.println("=======================================================");
			System.out.println();
			System.out.println();
		}
		
		for(int i = 0; i < cacheSizes.size(); i++) {
			System.out.println("Cache size is set to "+cacheSizes.get(i));
			System.out.println("GRAPHDATABASE: Neo4j");
			System.out.println("DATASET:"+SYNTH_DATASET);
			System.out.println("CACHE: "+cacheSizes.get(i));
			titanClusteringBenchmark(cacheSizes.get(i), GraphDatabaseBenchmark.TITAN_SYNTHETIC_GRAPH);
			System.out.println("=======================================================");
			System.out.println();
			System.out.println();
		}
		
	}
	
	public void titanClusteringBenchmark(int cacheSize, String dbPAth) throws ExecutionException {
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		titanGraphDatabase.open(dbPAth);
		for(int i = 0; i < 2; i++) {
			System.out.println("Loop "+(i+1));
			long start = System.currentTimeMillis();
			LouvainMethodCache louvainMethodCache = new LouvainMethodCache(titanGraphDatabase, cacheSize, IS_RANDOMIZED);
			louvainMethodCache.computeModularity();
			long titanTime = System.currentTimeMillis() - start;
			System.out.println("TIME: "+titanTime / 1000.0);
			System.out.println();
			System.out.println();
		}
	}
	
	public void orientClusteringBenchmark(int cacheSize, String dbPAth) throws ExecutionException {
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		orientGraphDatabase.open(dbPAth);
		for(int i = 0; i < 2; i++) {
			System.out.println("Loop "+(i+1));
			long start = System.currentTimeMillis();
			LouvainMethodCache louvainMethodCache = new LouvainMethodCache(orientGraphDatabase, cacheSize, IS_RANDOMIZED);
			louvainMethodCache.computeModularity();
			long orientTime = System.currentTimeMillis() - start;
			System.out.println("TIME: "+orientTime / 1000.0);
			System.out.println();
			System.out.println();
		}
	}
	
	public void neo4jClusteringBenchmark(int cacheSize, String dbPAth) throws ExecutionException {
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.open(dbPAth);
		for(int i = 0; i < 2; i ++) {
			System.out.println("Loop "+(i+1));
			long start = System.currentTimeMillis();
			LouvainMethodCache louvainMethodCache = new LouvainMethodCache(neo4jGraphDatabase, cacheSize, IS_RANDOMIZED);
			louvainMethodCache.computeModularity();
			long neo4jTime = System.currentTimeMillis() - start;
			System.out.println("TIME: "+neo4jTime / 1000.0);
			System.out.println();
			System.out.println();
		}
		
	}
}
