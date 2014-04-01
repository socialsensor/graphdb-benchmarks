package eu.socialsensor.main;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import eu.socialsensor.benchmarks.Benchmark;
import eu.socialsensor.benchmarks.ClusteringBenchmark;
import eu.socialsensor.benchmarks.FindNeighboursOfAllNodesBenchmark;
import eu.socialsensor.benchmarks.FindNodesOfAllEdgesBenchmark;
import eu.socialsensor.benchmarks.FindShortestPathBenchmark;
import eu.socialsensor.benchmarks.MassiveInsertionBenchmark;
import eu.socialsensor.benchmarks.SingleInsertionBenchmark;
import eu.socialsensor.clustering.LouvainMethod;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.utils.Metrics;
import eu.socialsensor.utils.Utils;

/**
 * Main class for the execution of GraphDatabaseBenchmark.
 * Choose one of the following benchmarks by removing the comments
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 *
 */
@SuppressWarnings("unused")
public class GraphDatabaseBenchmark {
		
	public final static String ENRON_DATASET = "data/enronEdges.txt";
	public final static String AMAZON_DATASET = "data/amazonEdges.txt";
	public final static String YOUTUBE_DATASET = "data/youtubeEdges.txt";
	public final static String LIVEJOURNAL_DATASET = "data/livejournalEdges.txt";
	public final static String SYNTHETIC_GRAPH = "data/network.dat";
	
	public final static String ORIENTDB_PATH = "data/OrientDB";
	public final static String TITANDB_PATH = "data/TitanDB";
	public final static String NEO4JDB_PATH = "data/Neo4jDB";
	
	/**
	 * This is the main function. Before you run the project un-comment
	 * lines that correspond to the benchmark you want to run.
	 * @throws ExecutionException 
	 */
	public static void main(String[] args) throws ExecutionException {
		
		/**
		 * MIW: Uncomment, choose dataset and run
		 */
//		Benchmark massiveInsertionBenchmark = new MassiveInsertionBenchmark(SYNTHETIC_GRAPH);
//		massiveInsertionBenchmark.startBenchmark();
		
		
		/**
		 * SIW: Uncomment, choose dataset and run
		 */
//		Benchmark singleInsertionBenchmark = new SingleInsertionBenchmark(ENRON_DATASET);
//		singleInsertionBenchmark.startBenchmark();
		
		
		/*
		 * For the other workloads you need to creat the graph database firstly.
		 * Uncoment, choose dataset and run.
		 */
//		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
//		titanGraphDatabase.createGraphForMassiveLoad(TITANDB_PATH);
//		titanGraphDatabase.massiveModeLoading(SYNTHETIC_GRAPH);
//		titanGraphDatabase.shutdownMassiveGraph();
//		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
//		orientGraphDatabase.createGraphForMassiveLoad(ORIENTDB_PATH);
//		orientGraphDatabase.massiveModeLoading(SYNTHETIC_GRAPH);
//		orientGraphDatabase.shutdownMassiveGraph();
//		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
//		neo4jGraphDatabase.createGraphForMassiveLoad(NEO4JDB_PATH);
//		neo4jGraphDatabase.massiveModeLoading(SYNTHETIC_GRAPH);
//		neo4jGraphDatabase.shutdownMassiveGraph();
		
		
		/**
		 * QW: If you have created the databases, just uncomment and run
		 */
//		Benchmark findNeighboursOfAllNodesBenchmark = new FindNeighboursOfAllNodesBenchmark();
//		findNeighboursOfAllNodesBenchmark.startBenchmark();
//		Benchmark findNodesOfAllEdgesBenchmark = new FindNodesOfAllEdgesBenchmark();
//		findNeighboursOfAllNodesBenchmark.startBenchmark();
//		Benchmark findShortestPathBenchmark = new FindShortestPathBenchmark();
//		findShortestPathBenchmark.startBenchmark();
		
		
		/**
		 * CW: If you have created the databases, just uncomment and run
		 */
//		Benchmark clusteringBenchmark = new ClusteringBenchmark();
//		clusteringBenchmark.startBenchmark();
		
	}
	
	

}
