package eu.socialsensor.main;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import eu.socialsensor.benchmarks.ClusteringBenchmark;
import eu.socialsensor.benchmarks.FindNeighboursOfAllNodesBenchmark;
import eu.socialsensor.benchmarks.FindNodesOfAllEdgesBenchmark;
import eu.socialsensor.benchmarks.FindShortestPathBenchmark;
import eu.socialsensor.benchmarks.MassiveInsertionBenchmark;
import eu.socialsensor.benchmarks.SingleInsertionBenchmark;
import eu.socialsensor.clustering.LouvainMethodCache;
import eu.socialsensor.clustering.LouvainMethodCache2;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.utils.Metrics;
import eu.socialsensor.utils.Utils;

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
//		MassiveInsertionBenchmark massiveInsertionBenchmark = new MassiveInsertionBenchmark(SYNTHETIC_GRAPH);
//		massiveInsertionBenchmark.startMassiveInsertionBenchmark();
		
		
		/**
		 * SIW: Uncomment, choose dataset and run
		 */
//		SingleInsertionBenchmark singleInsertionBenchmark = new SingleInsertionBenchmark(ENRON_DATASET);
//		singleInsertionBenchmark.startBenchmark();
		
		
		/*
		 * For the other workloads you need to creat the graph database firstly.
		 * Uncoment, choose dataset and run.
		 */
//		GraphDatabase titanGraphDatabaSE = NEW TITANGRAPHDATABASE();
//		TITANGRAPHDATABASE.CREATEGRAPHFORMASSIVELOAD(TITANDB_PATH);
//		TITANGRAPHDATABASE.MASSIVEMODELOADING(SYNTHETIC_GRAPH);
//		TITANGRAPHDATABASE.SHUTDOWN();
//		GRAPHDATABASE ORIENTGRAPHDATABASE = NEW ORIENTGRAPHDATABASE();
//		ORIENTGRAPHDATABASE.CREATEGRAPHFORMASSIVELOAD(ORIENTDB_PATH);
//		ORIENTGRAPHDATABASE.MASSIVEMODELOADING(SYNTHETIC_GRAPH);
//		ORIENTGRAPHDATABASE.SHUTDOWN();
//		GRAPHDATABASE NEO4JGRAPHDATABASE = NEW NEO4JGRAPHDATABASE();
//		NEO4JGRAPHDATABASE.CREATEGRAPHFORMASSIVELOAD(NEO4JDB_PATH);
//		NEO4JGRAPHDATABASE.MASSIVEMODELOADING(SYNTHETIC_GRAPH);
//		NEO4JGRAPHDATABASE.SHUTDOWNMASSIVEGRAPh();
		
		
		/**
		 * QW: If you have created the databases, just uncomment and run
		 */
//		FindNeighboursOfAllNodesBenchmark findNeighboursOfAllNodesBenchmark = new FindNeighboursOfAllNodesBenchmark();
//		findNeighboursOfAllNodesBenchmark.startBenchmark();
//		FindNodesOfAllEdgesBenchmark findNodesOfAllEdgesBenchmark = new FindNodesOfAllEdgesBenchmark();
//		findNeighboursOfAllNodesBenchmark.startBenchmark();
//		FindShortestPathBenchmark findShortestPathBenchmark = new FindShortestPathBenchmark();
//		findShortestPathBenchmark.startBenchmark();
		
		
		/**
		 * CW: If you have created the databases, just uncomment and run
		 */
//		ClusteringBenchmark clusteringBenchmark = new ClusteringBenchmark();
//		clusteringBenchmark.startBenchmark();
		
	}
	
	

}
