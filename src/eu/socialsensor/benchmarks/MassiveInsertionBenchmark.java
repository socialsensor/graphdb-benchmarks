package eu.socialsensor.benchmarks;

import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.SparkseeGraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.main.GraphDatabaseBenchmark;
import eu.socialsensor.utils.PermuteMethod;
import eu.socialsensor.utils.Utils;

/**
 * MassiveInsertionBenchmark implementation
 * @author sotbeis
 * @email sotbeis@iti.gr
 */

public class MassiveInsertionBenchmark implements Benchmark{
	
	private static final String MIW_RESULTS = "MIWResults.txt";
	private String datasetDir;
	
	private double[] orientTimes = new double[GraphDatabaseBenchmark.SCENARIOS];
	private double[] titanTimes = new double[GraphDatabaseBenchmark.SCENARIOS];
	private double[] neo4jTimes = new double[GraphDatabaseBenchmark.SCENARIOS];
	private double[] sparkseeTimes = new double[GraphDatabaseBenchmark.SCENARIOS];
	
	private int titanScenarioCount = 0;
	private int orientScenarioCount = 0;
	private int neo4jScenarioCount = 0;
	private int sparkseeScenarioCount = 0;
	
	private Logger logger = Logger.getLogger(MassiveInsertionBenchmark.class);
	
	public MassiveInsertionBenchmark(String datasetDir) {
		this.datasetDir = datasetDir;
	}
	
	@Override
	public void startBenchmark() {
		logger.setLevel(Level.INFO);
		System.out.println("");
		logger.info("Executing Massive Insertion Benchmark . . . .");
		
		Utils utils = new Utils();
		Class<MassiveInsertionBenchmark> c = MassiveInsertionBenchmark.class;
		Method[] methods = utils.filter(c.getDeclaredMethods(), "MassiveInsertionBenchmark");
		PermuteMethod permutations = new PermuteMethod(methods);
		int cntPermutations = 1;
		while(permutations.hasNext()) {
			System.out.println("");
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
		logger.info("Massive Insertion Benchmark finished");
		
		utils.writeResults(titanTimes, orientTimes, neo4jTimes, sparkseeTimes, MIW_RESULTS, 
				"Massive Insertion");
	}
	
	@SuppressWarnings("unused")
	private void orientdbMassiveInsertionBenchmark() {
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		long start = System.currentTimeMillis();
		orientGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.ORIENTDB_PATH);
		orientGraphDatabase.massiveModeLoading(datasetDir);
		orientGraphDatabase.shutdownMassiveGraph();
		long orientTime = System.currentTimeMillis() - start;
		orientGraphDatabase.delete(GraphDatabaseBenchmark.ORIENTDB_PATH);
		orientTimes[orientScenarioCount] = orientTime / 1000.0;
		orientScenarioCount++;
	}

	@SuppressWarnings("unused")
	private void titanMassiveInsertionBenchmark() {
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		long start = System.currentTimeMillis();
		titanGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.TITANDB_PATH);
		titanGraphDatabase.massiveModeLoading(datasetDir);
		titanGraphDatabase.shutdownMassiveGraph();
		long titanTime = System.currentTimeMillis() - start;
		titanGraphDatabase.delete(GraphDatabaseBenchmark.TITANDB_PATH);
		titanTimes[titanScenarioCount] = titanTime / 1000.0;
		titanScenarioCount++;
	}
	
	@SuppressWarnings("unused")
	private void neo4jMassiveInsertionBenchmark() {
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		long start = System.currentTimeMillis();
		neo4jGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.NEO4JDB_PATH);
		neo4jGraphDatabase.massiveModeLoading(datasetDir);
		neo4jGraphDatabase.shutdownMassiveGraph();
		long neo4jTime = System.currentTimeMillis() - start;
		neo4jGraphDatabase.delete(GraphDatabaseBenchmark.NEO4JDB_PATH);
		neo4jTimes[neo4jScenarioCount] = neo4jTime / 1000.0;
		neo4jScenarioCount++;
	}
	
	@SuppressWarnings("unused")
	private void sparkseeMassiveInsertionBenchmark() {
		GraphDatabase sparkseeGraphDatabase = new SparkseeGraphDatabase();
		
		sparkseeGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.SPARKSEEDB_PATH);
		long start = System.currentTimeMillis();
		sparkseeGraphDatabase.massiveModeLoading(datasetDir);
		long sparkseeTime = System.currentTimeMillis() - start;
		sparkseeGraphDatabase.shutdownMassiveGraph();		
		sparkseeGraphDatabase.delete(GraphDatabaseBenchmark.SPARKSEEDB_PATH);
		sparkseeTimes[sparkseeScenarioCount] = sparkseeTime / 1000.0;
		sparkseeScenarioCount++;
	}
	
}
