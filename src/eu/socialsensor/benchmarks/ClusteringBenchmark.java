package eu.socialsensor.benchmarks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import eu.socialsensor.clustering.LouvainMethodCache;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;


public class ClusteringBenchmark {
	
	private final static String orientDBDir = "data/OrientDB";
	private final static String titanDBDir = "data/TitanDB";
	private final static String neo4jDBDir = "data/Neo4jDB";
	
	private final List<Integer> enronCacheSizes;
	
	public final static int CACHE_SIZE = 100;
	
	public final static boolean IS_RANDOMIZED = false;
	
	
	public ClusteringBenchmark() {
		
		enronCacheSizes = new ArrayList<Integer>(
				Arrays.asList(1836, 3670, 5504, 7339, 9173, 11008, 12843));
		
	}
	
	public void startBenchmark() throws ExecutionException {
		
		System.out.println("###########################################################");
		System.out.println("############## Starting Clustering Benchmark ##############");
		System.out.println("###########################################################");
		
		for(int i = 0; i < enronCacheSizes.size(); i++) {
			System.out.println("Cache size is set to "+enronCacheSizes.get(i));
			double neo4jTime = neo4jClusteringBenchmark(enronCacheSizes.get(i));
			System.out.println("GRAPHDATABASE: Neo4j");
			System.out.println("DATASET: Enron");
			System.out.println("CACHE: "+enronCacheSizes.get(i));
			System.out.println("TIME: "+neo4jTime);
			System.out.println("=======================================================");
			System.out.println();
			System.out.println();
		}
		
		for(int i = 0; i < enronCacheSizes.size(); i++) {
			System.out.println("Cache size is set to "+enronCacheSizes.get(i));
			double orientTime = orientClusteringBenchmark(enronCacheSizes.get(i));
			System.out.println("GRAPHDATABASE: OrientDB");
			System.out.println("DATASET: Enron");
			System.out.println("CACHE: "+enronCacheSizes.get(i));
			System.out.println("TIME: "+orientTime);
			System.out.println("=======================================================");
			System.out.println();
			System.out.println();
		}
		
		for(int i = 0; i < enronCacheSizes.size(); i++) {
			System.out.println("Cache size is set to "+enronCacheSizes.get(i));
			double titanTime = titanClusteringBenchmark(enronCacheSizes.get(i));
			System.out.println("GRAPHDATABASE: Neo4j");
			System.out.println("DATASET: Enron");
			System.out.println("CACHE: "+enronCacheSizes.get(i));
			System.out.println("TIME: "+titanTime);
			System.out.println("=======================================================");
			System.out.println();
			System.out.println();
		}
		
	}
	
	public double titanClusteringBenchmark(int cacheSize) throws ExecutionException {
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		titanGraphDatabase.open(titanDBDir);
		long start = System.currentTimeMillis();
		LouvainMethodCache louvainMethodCache = new LouvainMethodCache(titanGraphDatabase, cacheSize, IS_RANDOMIZED);
		louvainMethodCache.computeModularity();
		long titanTime = System.currentTimeMillis() - start;
		return titanTime/1000.0;
	}
	
	public double orientClusteringBenchmark(int cacheSize) throws ExecutionException {
		GraphDatabase orientGraphDatabase = new TitanGraphDatabase();
		orientGraphDatabase.open(orientDBDir);
		long start = System.currentTimeMillis();
		LouvainMethodCache louvainMethodCache = new LouvainMethodCache(orientGraphDatabase, cacheSize, IS_RANDOMIZED);
		louvainMethodCache.computeModularity();
		long orientTime = System.currentTimeMillis() - start;
		return orientTime/1000.0;
	}
	
	public double neo4jClusteringBenchmark(int cacheSize) throws ExecutionException {
		GraphDatabase neo4jGraphDatabase = new TitanGraphDatabase();
		neo4jGraphDatabase.open(neo4jDBDir);
		long start = System.currentTimeMillis();
		LouvainMethodCache louvainMethodCache = new LouvainMethodCache(neo4jGraphDatabase, cacheSize, IS_RANDOMIZED);
		louvainMethodCache.computeModularity();
		long neo4jTime = System.currentTimeMillis() - start;
		return neo4jTime /1000.0;
	}
}
