package eu.socialsensor.benchmarks;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.insert.Neo4jSingleInsertion;
import eu.socialsensor.insert.OrientSingleInsertion;
import eu.socialsensor.insert.TitanSingleInsertion;
import eu.socialsensor.main.GraphDatabaseBenchmark;
import eu.socialsensor.utils.Utils;

/**
 * SingleInsertionBenchmak implementation
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 */
public class SingleInsertionBenchmark implements Benchmark {
	
	public static int SCENARIOS = 2;
	public static String INSERTION_TIMES_OUTPUT_PATH;
	
	private String INSERTION_TIMES_OUTPUT_FILE = "SIWResults";
	private String DATASET_PATH;
		
	private Logger logger = Logger.getLogger(SingleInsertionBenchmark.class);
	
	public SingleInsertionBenchmark(String datasetPath) {
		this.DATASET_PATH = datasetPath;
	}
	
	@Override
	public void startBenchmark() {
		logger.setLevel(Level.INFO);
		logger.info("Executing Massive Insertion Benchmark . . . .");
		
		String resultsFolder = GraphDatabaseBenchmark.inputPropertiesFile.getProperty("RESULTS_PATH");
		INSERTION_TIMES_OUTPUT_PATH = resultsFolder + INSERTION_TIMES_OUTPUT_FILE;
		
		//Scenario 1
		logger.info("Scenario 1");
		titanSingleInsertionBenchmark();
		orientSingleInsertionBenchmark();
		neo4jSinglesInsertionBenchmark();

		//Scenario 2
		logger.info("Scenario 2");
		titanSingleInsertionBenchmark();
		neo4jSinglesInsertionBenchmark();
		orientSingleInsertionBenchmark();

		//Scenario 3
		logger.info("Scenario 3");
		orientSingleInsertionBenchmark();
		titanSingleInsertionBenchmark();
		neo4jSinglesInsertionBenchmark();

		//Scenario 4
		logger.info("Scenario 4");
		orientSingleInsertionBenchmark();
		neo4jSinglesInsertionBenchmark();
		titanSingleInsertionBenchmark();

		//Scenario 5
		logger.info("Scenario 5");
		neo4jSinglesInsertionBenchmark();
		titanSingleInsertionBenchmark();
		orientSingleInsertionBenchmark();

		//Scenario 6
		logger.info("Scenario 6");
		neo4jSinglesInsertionBenchmark();
		orientSingleInsertionBenchmark();
		titanSingleInsertionBenchmark();
		
		logger.info("Single Insertion Benchmark finished");
		
		Utils utils = new Utils();
		List<List<Double>> titanInsertionTimesOfEachScenario = new ArrayList<List<Double>>(SingleInsertionBenchmark.SCENARIOS);
		utils.getDocumentsAs2dList(titanInsertionTimesOfEachScenario, TitanSingleInsertion.INSERTION_TIMES_OUTPUT_PATH);
		List<List<Double>> orientInsertionTimesOfEachScenario = new ArrayList<List<Double>>(SingleInsertionBenchmark.SCENARIOS);
		utils.getDocumentsAs2dList(orientInsertionTimesOfEachScenario, OrientSingleInsertion.INSERTION_TIMES_OUTPUT_PATH);
		List<List<Double>> neo4jInsertionTimesOfEachScenario = new ArrayList<List<Double>>(SingleInsertionBenchmark.SCENARIOS);
		utils.getDocumentsAs2dList(neo4jInsertionTimesOfEachScenario, Neo4jSingleInsertion.INSERTION_TIMES_OUTPUT_PATH);
		
		//compute mean time
		List<Double> titanMeanInsertionTimes = utils.calculateMeanList(titanInsertionTimesOfEachScenario);
		List<Double> orientMeanInsertionTimes = utils.calculateMeanList(orientInsertionTimesOfEachScenario);
		List<Double> neo4jMeanInsertionTimes = utils.calculateMeanList(neo4jInsertionTimesOfEachScenario);
		
		logger.info("Write results to "+resultsFolder);
		utils.writeTimes(titanMeanInsertionTimes, TitanSingleInsertion.INSERTION_TIMES_OUTPUT_PATH);
		utils.writeTimes(orientMeanInsertionTimes, OrientSingleInsertion.INSERTION_TIMES_OUTPUT_PATH);
		utils.writeTimes(neo4jMeanInsertionTimes, Neo4jSingleInsertion.INSERTION_TIMES_OUTPUT_PATH);
		
		//clearing thrash
		utils.deleteMultipleFiles(TitanSingleInsertion.INSERTION_TIMES_OUTPUT_PATH, SingleInsertionBenchmark.SCENARIOS);
		utils.deleteMultipleFiles(OrientSingleInsertion.INSERTION_TIMES_OUTPUT_PATH, SingleInsertionBenchmark.SCENARIOS);
		utils.deleteMultipleFiles(Neo4jSingleInsertion.INSERTION_TIMES_OUTPUT_PATH, SingleInsertionBenchmark.SCENARIOS);
	}
	
	private void titanSingleInsertionBenchmark() {
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		titanGraphDatabase.createGraphForSingleLoad(GraphDatabaseBenchmark.TITANDB_PATH);
		titanGraphDatabase.singleModeLoading(DATASET_PATH);
		titanGraphDatabase.shutdown();
		titanGraphDatabase.delete(GraphDatabaseBenchmark.TITANDB_PATH);
	}
		
	private void orientSingleInsertionBenchmark() {
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		orientGraphDatabase.createGraphForSingleLoad(GraphDatabaseBenchmark.ORIENTDB_PATH);
		orientGraphDatabase.singleModeLoading(DATASET_PATH);
		orientGraphDatabase.shutdown();
		orientGraphDatabase.delete(GraphDatabaseBenchmark.ORIENTDB_PATH);
	}
	
	private void neo4jSinglesInsertionBenchmark() {
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.createGraphForSingleLoad(GraphDatabaseBenchmark.NEO4JDB_PATH);
		neo4jGraphDatabase.singleModeLoading(DATASET_PATH);
		neo4jGraphDatabase.shutdown();
		neo4jGraphDatabase.delete(GraphDatabaseBenchmark.NEO4JDB_PATH);
	}

}
