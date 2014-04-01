package eu.socialsensor.benchmarks;

import java.util.ArrayList;
import java.util.List;

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
	
	public static int SCENARIOS = 6;
	
	private String DATASET_PATH;
	
	public SingleInsertionBenchmark(String datasetPath) {
		this.DATASET_PATH = datasetPath;
	}
	
	@Override
	public void startBenchmark() {
		System.out.println("###########################################################");
		System.out.println("############ Starting Single Insertion Benchmark ############");
		System.out.println("###########################################################");
	
		//Scenario 1
		System.out.println("##################### Running scenario 1 #####################");
		titanSingleInsertionBenchmark();
		orientSingleInsertionBenchmark();
		neo4jSinglesInsertionBenchmark();

		//Scenario 2
		System.out.println("##################### Running scenario 2 #####################");
		titanSingleInsertionBenchmark();
		neo4jSinglesInsertionBenchmark();
		orientSingleInsertionBenchmark();

		//Scenario 3
		System.out.println("##################### Running scenario 3 #####################");
		orientSingleInsertionBenchmark();
		titanSingleInsertionBenchmark();
		neo4jSinglesInsertionBenchmark();

		//Scenario 4
		System.out.println("##################### Running scenario 4 #####################");
		orientSingleInsertionBenchmark();
		neo4jSinglesInsertionBenchmark();
		titanSingleInsertionBenchmark();

		//Scenario 5
		System.out.println("##################### Running scenario 5 #####################");
		neo4jSinglesInsertionBenchmark();
		titanSingleInsertionBenchmark();
		orientSingleInsertionBenchmark();

		//Scenario 6
		System.out.println("##################### Running scenario 6 #####################");
		neo4jSinglesInsertionBenchmark();
		orientSingleInsertionBenchmark();
		titanSingleInsertionBenchmark();
		
		Utils utils = new Utils();
		List<List<Double>> titanInsertionTimesOfEachScenario = new ArrayList<List<Double>>(SingleInsertionBenchmark.SCENARIOS);
		utils.getDocumentsAs2dList(titanInsertionTimesOfEachScenario, TitanSingleInsertion.INSERTION_TIMES_OUTPUT_PATH);
		List<List<Double>> orientInsertionTimesOfEachScenario = new ArrayList<List<Double>>(SingleInsertionBenchmark.SCENARIOS);
		utils.getDocumentsAs2dList(orientInsertionTimesOfEachScenario, OrientSingleInsertion.INSERTION_TIMES_OUTPUT_PATH);
		List<List<Double>> neo4jInsertionTimesOfEachScenario = new ArrayList<List<Double>>(SingleInsertionBenchmark.SCENARIOS);
		utils.getDocumentsAs2dList(neo4jInsertionTimesOfEachScenario, Neo4jSingleInsertion.INSERTION_TIMES_OUTPUT_PATH);
		
		System.out.println("Finding mean values . . . .");
		List<Double> titanMeanInsertionTimes = utils.calculateMeanList(titanInsertionTimesOfEachScenario);
		List<Double> orientMeanInsertionTimes = utils.calculateMeanList(orientInsertionTimesOfEachScenario);
		List<Double> neo4jMeanInsertionTimes = utils.calculateMeanList(neo4jInsertionTimesOfEachScenario);
		
		System.out.println("Writing values . . . .");
		utils.writeTimes(titanMeanInsertionTimes, "data/titanInsertionTimes");
		utils.writeTimes(orientMeanInsertionTimes, "data/orientInsertionTimes");
		utils.writeTimes(neo4jMeanInsertionTimes, "data/neo4jInsertionTimes");
		
		System.out.println("Clearing thrash . . . .");
		utils.deleteMultipleFiles(TitanSingleInsertion.INSERTION_TIMES_OUTPUT_PATH, SingleInsertionBenchmark.SCENARIOS);
		utils.deleteMultipleFiles(OrientSingleInsertion.INSERTION_TIMES_OUTPUT_PATH, SingleInsertionBenchmark.SCENARIOS);
		utils.deleteMultipleFiles(Neo4jSingleInsertion.INSERTION_TIMES_OUTPUT_PATH, SingleInsertionBenchmark.SCENARIOS);
		
		System.out.println("#############################################################");
		System.out.println("############ Single Insertion Benchmark Finished ############");
		System.out.println("#############################################################");
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
