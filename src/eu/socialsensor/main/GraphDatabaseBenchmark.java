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
	
	public final static String TITAN_SYNTHETIC_GRAPH = "data/titan";
	public final static String ORIENT_SYNTHETIC_GRAPH = "data/orient";
	public final static String NEO4J_SYNTHETIC_GRAPH = "data/neo4j";
	
	/**
	 * This is the main function. Before you run the project un-comment
	 * lines that correspond to the benchmark you want to run.
	 * @throws ExecutionException 
	 */
	public static void main(String[] args) throws ExecutionException {
				
		ClusteringBenchmark clusteringBenchmark = new ClusteringBenchmark();
		clusteringBenchmark.startBenchmark();
		
//		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
//		titanGraphDatabase.createGraphForMassiveLoad("data/titan");
//		titanGraphDatabase.massiveModeLoading("data/network.dat");
//		titanGraphDatabase.shutdownMassiveGraph();
//		
//		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
//		orientGraphDatabase.createGraphForMassiveLoad("data/orient");
//		orientGraphDatabase.massiveModeLoading("data/network.dat");
//		orientGraphDatabase.shutdownMassiveGraph();
//		
//		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
//		neo4jGraphDatabase.createGraphForMassiveLoad("data/neo4j");
//		neo4jGraphDatabase.massiveModeLoading("data/network.dat");
//		neo4jGraphDatabase.shutdownMassiveGraph();
		
	}
	
	

}
