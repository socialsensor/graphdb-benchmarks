package eu.socialsensor.benchmarks;

import insert.Neo4jSingleInsertion;
import insert.OrientSingleInsertion;
import insert.SingleInsertion;
import insert.TitanSingleInsertion;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import eu.socialsensor.utils.Utils;

public class SingleInsertionBenchmark {
	
	private final static String orientDBDir = "data/OrientDB";
	private final static String titanDBDir = "data/TitanDB";
	private final static String neo4jDBDir = "data/Neo4jDB";
	
	private String datasetDir;
	
	public SingleInsertionBenchmark(String datasetDir) {
		this.datasetDir = datasetDir;
	}
	
	public void startBenchmark() {
		System.out.println("###########################################################");
		System.out.println("############ Starting Single Insertion Benchmark ############");
		System.out.println("###########################################################");
		
		List<List<Double>> titanInsertionTimesOfEachScenario = new ArrayList<List<Double>>();
		List<List<Double>> orientInsertionTimesOfEachScenario = new ArrayList<List<Double>>();
		List<List<Double>> neo4jInsertionTimesOfEachScenario = new ArrayList<List<Double>>();
		
		//Scenario 1
		System.out.println("##################### Running scenario 1 #####################");
		titanInsertionTimesOfEachScenario.add(titanSingleInsertionBenchmark());
		orientInsertionTimesOfEachScenario.add(orientSingleInsertionBenchmark());
		neo4jInsertionTimesOfEachScenario.add(neo4jSinglesInsertionBenchmark());
		
		//Scenario 2
		System.out.println("##################### Running scenario 2 #####################");
		titanInsertionTimesOfEachScenario.add(titanSingleInsertionBenchmark());
		neo4jInsertionTimesOfEachScenario.add(neo4jSinglesInsertionBenchmark());
		orientInsertionTimesOfEachScenario.add(orientSingleInsertionBenchmark());
				
		//Scenario 3
		System.out.println("##################### Running scenario 3 #####################");
		orientInsertionTimesOfEachScenario.add(orientSingleInsertionBenchmark());
		titanInsertionTimesOfEachScenario.add(titanSingleInsertionBenchmark());
		neo4jInsertionTimesOfEachScenario.add(neo4jSinglesInsertionBenchmark());
				
		//Scenario 4
		System.out.println("##################### Running scenario 4 #####################");
		orientInsertionTimesOfEachScenario.add(orientSingleInsertionBenchmark());
		neo4jInsertionTimesOfEachScenario.add(neo4jSinglesInsertionBenchmark());
		titanInsertionTimesOfEachScenario.add(titanSingleInsertionBenchmark());
				
		//Scenario 5
		System.out.println("##################### Running scenario 5 #####################");
		neo4jInsertionTimesOfEachScenario.add(neo4jSinglesInsertionBenchmark());
		titanInsertionTimesOfEachScenario.add(titanSingleInsertionBenchmark());
		orientInsertionTimesOfEachScenario.add(orientSingleInsertionBenchmark());
				
		//Scenario 6
		System.out.println("##################### Running scenario 6 #####################");
		neo4jInsertionTimesOfEachScenario.add(neo4jSinglesInsertionBenchmark());
		orientInsertionTimesOfEachScenario.add(orientSingleInsertionBenchmark());
		titanInsertionTimesOfEachScenario.add(titanSingleInsertionBenchmark());
		
		Utils utils = new Utils();
		System.out.println("Finding mean values . . . .");
		List<Double> titanMeanInsertionTimes = utils.calculateMeanList(titanInsertionTimesOfEachScenario);
		List<Double> orientMeanInsertionTimes = utils.calculateMeanList(orientInsertionTimesOfEachScenario);
		List<Double> neo4jMeanInsertionTimes = utils.calculateMeanList(neo4jInsertionTimesOfEachScenario);
		
		System.out.println("Writing values . . . .");
		utils.writeTimes(titanMeanInsertionTimes, "data/titanInsertionTimes");
		utils.writeTimes(orientMeanInsertionTimes, "data/orientInsertionTimes");
		utils.writeTimes(neo4jMeanInsertionTimes, "data/neo4jInsertionTimes");
		
		System.out.println("###########################################################");
		System.out.println("############ Single Insertion Benchmark Finished ############");
		System.out.println("######################### RESULTS #########################");
		
	}
	
	public List<Double> titanSingleInsertionBenchmark() {
		SingleInsertion titanSingleInsertion = new TitanSingleInsertion();
		titanSingleInsertion.startup(titanDBDir);
		List<Double> titanInsertionTimes = titanSingleInsertion.createGraph(datasetDir);
		titanSingleInsertion.shutdown();
		try {
			Thread.sleep(6000);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		Utils utils = new Utils();
//		utils.writeTimes(titanInsertionTimes, "data/titanInsertionTimes");
		utils.deleteRecursively(new File(titanDBDir));
		return titanInsertionTimes;
	}
		
	public List<Double> orientSingleInsertionBenchmark() {
		SingleInsertion orientSingleInsertion = new OrientSingleInsertion();
		orientSingleInsertion.startup(orientDBDir);
		List<Double> orientInsertionTimes = orientSingleInsertion.createGraph(datasetDir);
		orientSingleInsertion.shutdown();
		try {
			Thread.sleep(6000);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		Utils utils = new Utils();
//		utils.writeTimes(orientInsertionTimes, "data/orientInsertionTimes");
		utils.deleteRecursively(new File(orientDBDir));
		return orientInsertionTimes;
	}
	
	public List<Double> neo4jSinglesInsertionBenchmark() {
		SingleInsertion neo4jSingleInsertion = new Neo4jSingleInsertion();
		neo4jSingleInsertion.startup(neo4jDBDir);
		List<Double> neo4jInsertionTimes = neo4jSingleInsertion.createGraph(datasetDir);
		neo4jSingleInsertion.shutdown();
		try {
			Thread.sleep(6000);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		Utils utils = new Utils();
//		utils.writeTimes(neo4jInsertionTimes, "data/neo4jInsertionTimes");
		utils.deleteRecursively(new File(neo4jDBDir));
		return neo4jInsertionTimes;
	}

}
