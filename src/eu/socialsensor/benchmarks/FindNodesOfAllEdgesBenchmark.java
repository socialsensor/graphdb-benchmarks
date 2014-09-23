package eu.socialsensor.benchmarks;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.SparkseeGraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.main.GraphDatabaseBenchmark;
import eu.socialsensor.utils.PermuteMethod;
import eu.socialsensor.utils.Utils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * FindNodesOfAllEdgesBenchmark implementation
 * @author sotbeis
 * @email sotbeis@iti.gr
 */
public class FindNodesOfAllEdgesBenchmark implements Benchmark {
	
	private static final String QW_FA_RESULTS = "QW-FAResults.txt";
	
	private Logger logger = Logger.getLogger(FindNodesOfAllEdgesBenchmark.class);
	
	private double[] orientTimes = new double[GraphDatabaseBenchmark.SCENARIOS];
	private double[] titanTimes = new double[GraphDatabaseBenchmark.SCENARIOS];
	private double[] neo4jTimes = new double[GraphDatabaseBenchmark.SCENARIOS];
	private double[] sparkseeTimes = new double[GraphDatabaseBenchmark.SCENARIOS];
	
	private int titanScenarioCount = 0;
	private int orientScenarioCount = 0;
	private int neo4jScenarioCount = 0;
	private int sparkseeScenarioCount = 0;
	
	@Override
	public void startBenchmark() {
		logger.setLevel(Level.INFO);
		System.out.println("");
		logger.info("Find Adjacent Nodes of All Edges Benchmark. . . .");
		
		Utils utils = new Utils();
		Class<FindNodesOfAllEdgesBenchmark> c = FindNodesOfAllEdgesBenchmark.class;
		Method[] methods = utils.filter(c.getDeclaredMethods(), "FindNodesOfAllEdgesBenchmark");
		PermuteMethod permutations = new PermuteMethod(methods);
		int cntPermutations = 1;
		while(permutations.hasNext()) {
			logger.info("Scenario " + cntPermutations++);
			for(Method permutation : permutations.next()) {
				try {
					permutation.invoke(this, null);
					utils.clearGC();
				} catch (IllegalAccessException | IllegalArgumentException
						| InvocationTargetException e) {
					e.printStackTrace();
				}
				
			}
		}
		
		System.out.println("");
		logger.info("Find Adjacent Nodes of All Edges Benchmark finished");
		
		utils.writeResults(titanTimes, orientTimes, neo4jTimes, sparkseeTimes, QW_FA_RESULTS, 
				"Find Adjacent Nodes of All Edges");
	}
	
	@SuppressWarnings("unused")
	private void orientdbFindNodesOfAllEdgesBenchmark() {
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		orientGraphDatabase.open(GraphDatabaseBenchmark.ORIENTDB_PATH);
		long start = System.currentTimeMillis();
		orientGraphDatabase.nodesOfAllEdgesQuery();
		long orientTime = System.currentTimeMillis() - start;
		orientGraphDatabase.shutdown();
		orientTimes[orientScenarioCount] = orientTime / 1000.0;
		orientScenarioCount++;
	}
	
	@SuppressWarnings("unused")
	private void titanFindNodesOfAllEdgesBenchmark() {
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		titanGraphDatabase.open(GraphDatabaseBenchmark.TITANDB_PATH);
		long start = System.currentTimeMillis();
		titanGraphDatabase.nodesOfAllEdgesQuery();
		long titanTime = System.currentTimeMillis() - start;
		titanGraphDatabase.shutdown();
		titanTimes[titanScenarioCount] = titanTime / 1000.0;
		titanScenarioCount++;
	}
	
	@SuppressWarnings("unused")
	private void neo4jFindNodesOfAllEdgesBenchmark() {
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.open(GraphDatabaseBenchmark.NEO4JDB_PATH);
		long start = System.currentTimeMillis();
		neo4jGraphDatabase.nodesOfAllEdgesQuery();
		long neo4jTime = System.currentTimeMillis() - start;
		neo4jGraphDatabase.shutdown();
		neo4jTimes[neo4jScenarioCount] = neo4jTime / 1000.0;
		neo4jScenarioCount++;
	}
	
	public void sparkseeFindNodesOfAllEdgesBenchmark() {
		GraphDatabase sparkseeGraphDatabase = new SparkseeGraphDatabase();
		sparkseeGraphDatabase.open(GraphDatabaseBenchmark.SPARKSEEDB_PATH);
		long start = System.currentTimeMillis();
		sparkseeGraphDatabase.nodesOfAllEdgesQuery();
		long sparkseeTime = System.currentTimeMillis() - start;
		sparkseeGraphDatabase.shutdown();
		sparkseeTimes[sparkseeScenarioCount] = sparkseeTime / 1000.0;
		sparkseeScenarioCount++;
	}

}
