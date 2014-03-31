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
				Arrays.asList(400, 500));
	}
	
	public void startBenchmark() throws ExecutionException {
		
		System.out.println("###########################################################");
		System.out.println("############## Starting Clustering Benchmark ##############");
		System.out.println("###########################################################");
		
		neo4jClusteringBenchmark(GraphDatabaseBenchmark.NEO4JDB_PATH);
//		orientClusteringBenchmark(GraphDatabaseBenchmark.ORIENTDB_PATH);
//		titanClusteringBenchmark(GraphDatabaseBenchmark.TITANDB_PATH);
		
		System.out.println("###########################################################");
		System.out.println("############## Clustering Benchmark Finished ##############");
		System.out.println("###########################################################");
		
	}
	
	public void titanClusteringBenchmark(String dbPAth) throws ExecutionException {
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		titanGraphDatabase.open(dbPAth);
		int runs = 1;
		int numberOfLoops = 2;
		for(int i = 0; i < cacheSizes.size(); i++) {
			System.out.println("Cache size is set to "+cacheSizes.get(i));
			System.out.println("DATASET:"+SYNTH_DATASET);
			System.out.println("GRAPHDATABASE: Titan");
			System.out.println("CACHE: "+cacheSizes.get(i));
			System.out.println();
			if(runs > 1) {
				numberOfLoops = 1;
			}
			for(int j = 0; j < numberOfLoops; j++) {
				System.out.println("Loop "+(j+1));
				long start = System.currentTimeMillis();
				LouvainMethodCache louvainMethodCache = new LouvainMethodCache(titanGraphDatabase, cacheSizes.get(i), IS_RANDOMIZED);
				louvainMethodCache.computeModularity();
				long titanTime = System.currentTimeMillis() - start;
				System.out.println("TIME: "+titanTime / 1000.0);
				System.out.println();
			}
			System.out.println("=======================================================");
			System.out.println();
			runs++;
		}
	}
	
	public void orientClusteringBenchmark(String dbPAth) throws ExecutionException {
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		orientGraphDatabase.open(dbPAth);
		int runs = 1;
		int numberOfLoops = 2;
		for(int i = 0; i < cacheSizes.size(); i++) {
			System.out.println("Cache size is set to "+cacheSizes.get(i));
			System.out.println("GRAPHDATABASE: OrientDB");
			System.out.println("DATASET:"+SYNTH_DATASET);
			System.out.println("CACHE: "+cacheSizes.get(i));
			System.out.println();
			if(runs > 1) {
				numberOfLoops = 1;
			}
			for(int j = 0; j < numberOfLoops; j++) {
				System.out.println("Loop "+(j+1));
				long start = System.currentTimeMillis();
				LouvainMethodCache louvainMethodCache = new LouvainMethodCache(orientGraphDatabase, cacheSizes.get(i), IS_RANDOMIZED);
				louvainMethodCache.computeModularity();
				long orientTime = System.currentTimeMillis() - start;
				System.out.println("TIME: "+orientTime / 1000.0);
				System.out.println();
			}
			runs++;
			System.out.println("=======================================================");
			System.out.println();
		}
	}
	
	public void neo4jClusteringBenchmark(String dbPAth) throws ExecutionException {
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.open(dbPAth);
		int runs = 1;
		int numberOfLoops = 2;
		for(int i = 0; i < cacheSizes.size(); i++) {
			System.out.println("Cache size is set to "+cacheSizes.get(i));
			System.out.println("GRAPHDATABASE: Neo4j");
			System.out.println("DATASET:"+SYNTH_DATASET);
			System.out.println("CACHE: "+cacheSizes.get(i));
			System.out.println();
			if(runs > 1) {
				numberOfLoops = 1;
			}
			for(int j = 0; j < numberOfLoops; j ++) {
				System.out.println("Loop "+(j+1));
				long start = System.currentTimeMillis();
				LouvainMethodCache louvainMethodCache = new LouvainMethodCache(neo4jGraphDatabase, cacheSizes.get(i), IS_RANDOMIZED);
				louvainMethodCache.computeModularity();
				long neo4jTime = System.currentTimeMillis() - start;
				System.out.println("TIME: "+neo4jTime / 1000.0);
				System.out.println();
			}
			runs++;
			System.out.println("=======================================================");
			System.out.println();
		}
	}
	
}
