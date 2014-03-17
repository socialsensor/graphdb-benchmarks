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
			for(int j = 0; j < 2 ; j++) {
				System.out.println("LOOP "+(j+1));
				double neo4jTime = neo4jClusteringBenchmark(cacheSizes.get(i), GraphDatabaseBenchmark.NEO4J_SYNTHETIC_GRAPH);
				System.out.println("GRAPHDATABASE: Neo4j");
				System.out.println("DATASET: Enron");
				System.out.println("CACHE: "+cacheSizes.get(i));
				System.out.println("TIME: "+neo4jTime);
				System.out.println("=======================================================");
			}
			System.out.println("=======================================================");
			System.out.println("=======================================================");
			System.out.println();
			System.out.println();
		}
		
		for(int i = 0; i < cacheSizes.size(); i++) {
			System.out.println("Cache size is set to "+cacheSizes.get(i));
			for(int j = 0; j < 2 ; j++) {
				double orientTime = orientClusteringBenchmark(cacheSizes.get(i), GraphDatabaseBenchmark.ORIENT_SYNTHETIC_GRAPH);
				System.out.println("GRAPHDATABASE: OrientDB");
				System.out.println("DATASET: Enron");
				System.out.println("CACHE: "+cacheSizes.get(i));
				System.out.println("TIME: "+orientTime);
				System.out.println("=======================================================");
			}
			System.out.println("=======================================================");
			System.out.println("=======================================================");
			System.out.println();
			System.out.println();
		}
		
		for(int i = 0; i < cacheSizes.size(); i++) {
			System.out.println("Cache size is set to "+cacheSizes.get(i));
			for(int j = 0; j < 2 ; j++) {
				double titanTime = titanClusteringBenchmark(cacheSizes.get(i), GraphDatabaseBenchmark.TITAN_SYNTHETIC_GRAPH);
				System.out.println("GRAPHDATABASE: Neo4j");
				System.out.println("DATASET: Enron");
				System.out.println("CACHE: "+cacheSizes.get(i));
				System.out.println("TIME: "+titanTime);
				System.out.println("=======================================================");
			}
			System.out.println("=======================================================");
			System.out.println("=======================================================");
			System.out.println();
			System.out.println();
		}
		
	}
	
	public double titanClusteringBenchmark(int cacheSize, String dbPAth) throws ExecutionException {
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		titanGraphDatabase.open(dbPAth);
		long start = System.currentTimeMillis();
		LouvainMethodCache louvainMethodCache = new LouvainMethodCache(titanGraphDatabase, cacheSize, IS_RANDOMIZED);
		louvainMethodCache.computeModularity();
		long titanTime = System.currentTimeMillis() - start;
		return titanTime/1000.0;
	}
	
	public double orientClusteringBenchmark(int cacheSize, String dbPAth) throws ExecutionException {
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		orientGraphDatabase.open(dbPAth);
		long start = System.currentTimeMillis();
		LouvainMethodCache louvainMethodCache = new LouvainMethodCache(orientGraphDatabase, cacheSize, IS_RANDOMIZED);
		louvainMethodCache.computeModularity();
		long orientTime = System.currentTimeMillis() - start;
		return orientTime/1000.0;
	}
	
	public double neo4jClusteringBenchmark(int cacheSize, String dbPAth) throws ExecutionException {
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.open(dbPAth);
		long start = System.currentTimeMillis();
		LouvainMethodCache louvainMethodCache = new LouvainMethodCache(neo4jGraphDatabase, cacheSize, IS_RANDOMIZED);
		louvainMethodCache.computeModularity();
		long neo4jTime = System.currentTimeMillis() - start;
		return neo4jTime /1000.0;
	}
}
