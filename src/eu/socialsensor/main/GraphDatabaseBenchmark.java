package eu.socialsensor.main;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;

import eu.socialsensor.benchmarks.FindNeighboursOfAllNodesBenchmark;
import eu.socialsensor.benchmarks.MassiveInsertionBenchmark;
import eu.socialsensor.benchmarks.SingleInsertionBenchmark;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;

public class GraphDatabaseBenchmark {
		
	private final static String ENRON_DATASET = "data/enronEdges.txt";
	private final static String AMAZON_DATASET = "data/amazonEdges.txt";
	private final static String YOUTUBE_DATASET = "data/youtubeEdges.txt";
	private final static String LIVEJOURNAL_DATASET = "data/livejournalEdges.txt";
	
	/**
	 * This is the main function. Before you run the project un-comment
	 * lines that correspond to the benchmark you want to run.
	 */
	public static void main(String[] args) {
		
//		MassiveInsertionBenchmark test = new MassiveInsertionBenchmark(ENRON_DATASET);
//		test.startMassiveInsertionBenchmark();
		
//		SingleInsertionBenchmark test = new SingleInsertionBenchmark(ENRON_DATASET);
//		test.startBenchmark();
		
//		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
//		titanGraphDatabase.createGraphForMassiveLoad("data/titan1");
//		titanGraphDatabase.massiveModeLoading("data/network1.dat");
//		titanGraphDatabase.shutdownMassiveGraph();
		
//		titanGraphDatabase.createGraphForSingleLoad("data/titan");
//		titanGraphDatabase.singleModeLoading(ENRON_DATASET);
//		titanGraphDatabase.shutdown();
		
//		titanGraphDatabase.open("data/titan");
		
		
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		orientGraphDatabase.createGraphForMassiveLoad("data/orient");
		orientGraphDatabase.massiveModeLoading("data/network.dat");
		orientGraphDatabase.shutdownMassiveGraph();
		
//		orientGraphDatabase.createGraphForSingleLoad("data/orient");
//		orientGraphDatabase.singleModeLoading(ENRON_DATASET);
//		orientGraphDatabase.shutdown();
//		
//		orientGraphDatabase.open("data/orient");

		
		
//		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
//		neo4jGraphDatabase.createGraphForMassiveLoad("data/neo4j");
//		neo4jGraphDatabase.massiveModeLoading(ENRON_DATASET);
//		neo4jGraphDatabase.shutdownMassiveGraph();
		
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
