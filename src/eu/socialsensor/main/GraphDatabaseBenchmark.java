package eu.socialsensor.main;

import java.util.concurrent.ExecutionException;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;

import eu.socialsensor.benchmarks.ClusteringBenchmark;
import eu.socialsensor.benchmarks.FindNeighboursOfAllNodesBenchmark;
import eu.socialsensor.benchmarks.MassiveInsertionBenchmark;
import eu.socialsensor.benchmarks.SingleInsertionBenchmark;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;

public class GraphDatabaseBenchmark {
		
	public final static String ENRON_DATASET = "data/enronEdges.txt";
	public final static String AMAZON_DATASET = "data/amazonEdges.txt";
	public final static String YOUTUBE_DATASET = "data/youtubeEdges.txt";
	public final static String LIVEJOURNAL_DATASET = "data/livejournalEdges.txt";
	
	public final static String TITAN_ENRON = "data/titanEnron";
	public final static String ORIENT_ENRON = "data/orientEnron";
	public final static String NEO4J_ENRON = "data/neo4jEnron";
	
	public final static String TITAN_SYNTHETIC_GRAPH = "data/titanSyntheticGraph";
	public final static String ORIENT_SYNTHETIC_GRAPH = "data/orientSyntheticGraph";
	public final static String NEO4J_SYNTHETIC_GRAPH = "data/neo4jSyntheticGraph";
	
	/**
	 * This is the main function. Before you run the project un-comment
	 * lines that correspond to the benchmark you want to run.
	 * @throws ExecutionException 
	 */
	public static void main(String[] args) throws ExecutionException {
		
//		MassiveInsertionBenchmark test = new MassiveInsertionBenchmark(ENRON_DATASET);
//		test.startMassiveInsertionBenchmark();
		
//		SingleInsertionBenchmark test = new SingleInsertionBenchmark(ENRON_DATASET);
//		test.startBenchmark();
		
//		ClusteringBenchmark clusteringBenchmark = new ClusteringBenchmark();
//		clusteringBenchmark.startBenchmark();
		
//		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
//		titanGraphDatabase.createGraphForMassiveLoad("data/titan");
//		titanGraphDatabase.massiveModeLoading("data/network.dat");
//		titanGraphDatabase.shutdownMassiveGraph();
		
//		titanGraphDatabase.createGraphForSingleLoad("data/titan");
//		titanGraphDatabase.singleModeLoading(ENRON_DATASET);
//		titanGraphDatabase.shutdown();
		
//		titanGraphDatabase.open("data/titan");
		
		
//		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
//		orientGraphDatabase.createGraphForMassiveLoad("data/orient");
//		orientGraphDatabase.massiveModeLoading("data/network.dat");
//		orientGraphDatabase.shutdownMassiveGraph();
		
//		orientGraphDatabase.createGraphForSingleLoad("data/orient1");
//		orientGraphDatabase.singleModeLoading("data/network.dat");
//		orientGraphDatabase.shutdown();
//		
//		orientGraphDatabase.open("data/orient");

		
//		
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.createGraphForMassiveLoad("data/neo4j");
		neo4jGraphDatabase.massiveModeLoading("data/network.dat");
		neo4jGraphDatabase.shutdownMassiveGraph();
		
//		neo4jGraphDatabase.createGraphForSingleLoad("data/neo4j");
//		neo4jGraphDatabase.singleModeLoading(ENRON_DATASET);
//		neo4jGraphDatabase.shutdown();
		
//		neo4jGraphDatabase.open("data/neo4jC");
		
//		System.out.println(counter);
//		MassiveInsertionBenchmark massiveInsertionBenchmark = new MassiveInsertionBenchmark("data/test.txt");
//		massiveInsertionBenchmark.startMassiveBenchmark();		
		
//		SingleInsertionBenchmark singleInsertionBenchmark = new SingleInsertionBenchmark("data/test.txt");
//		singleInsertionBenchmark.startBenchmark();
		
//		FindNeighboursOfAllNodesBenchmark findNeighboursOfAllNodesBenchmark = new FindNeighboursOfAllNodesBenchmark();
//		findNeighboursOfAllNodesBenchmark.startBenchmark();
		
//		FindNodesOfAllEdgesBenchmark findNodesOfAllEdgesBenchmark = new FindNodesOfAllEdgesBenchmark();
//		findNodesOfAllEdgesBenchmark.startBenchmark();
//		
//		FindShortestPathBenchmark findShortestPathBenchmark = new FindShortestPathBenchmark();
//		findShortestPathBenchmark.startBenchmark();
	}
	
	

}
