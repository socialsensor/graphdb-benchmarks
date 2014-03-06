package eu.socialsensor.benchmarks;

import java.io.File;

import eu.socialsensor.main.GraphDatabase;
import eu.socialsensor.main.Neo4jGraphDatabase;
import eu.socialsensor.main.OrientGraphDatabase;
import eu.socialsensor.main.TitanGraphDatabase;
import eu.socialsensor.utils.Utils;

public class MassiveInsertionBenchmark {
	
	private final static String orientDBDir = "data/OrientDB";
	private final static String titanDBDir = "data/TitanDB";
	private final static String neo4jDBDir = "data/Neo4jDB";
	
	private String datasetDir;
	
	public MassiveInsertionBenchmark(String datasetDir) {
		this.datasetDir = datasetDir;
	}
	
	public void startMassiveInsertionBenchmark() {
		System.out.println("###########################################################");
		System.out.println("############ Starting Massive Insertion Benchmark ############");
		System.out.println("###########################################################");
		
		double[] orientTimes = new double[6];
		double[] titanTimes = new double[6];
		double[] neo4jTimes = new double[6];
		
		//Scenario 1
		System.out.println("##################### Running scenario 1 #####################");
		orientTimes[0] = orientMassiveInsertionBenchmark();
		titanTimes[0] = titanMassiveInsertionBenchmark();
		neo4jTimes[0] = neo4jMassiveInsertionBenchmark();
		
		//Scenario 2
		System.out.println("##################### Running scenario 2 #####################");
		orientTimes[1] = orientMassiveInsertionBenchmark();
		neo4jTimes[1] = neo4jMassiveInsertionBenchmark();
		titanTimes[1] = titanMassiveInsertionBenchmark();
		
		//Scenario 3
		System.out.println("##################### Running scenario 3 #####################");
		neo4jTimes[2] = neo4jMassiveInsertionBenchmark();
		orientTimes[2] = orientMassiveInsertionBenchmark();
		titanTimes[2] = titanMassiveInsertionBenchmark();
		
		//Scenario 4
		System.out.println("##################### Running scenario 4 #####################");
		neo4jTimes[3] = neo4jMassiveInsertionBenchmark();
		titanTimes[3] = titanMassiveInsertionBenchmark();
		orientTimes[3] = orientMassiveInsertionBenchmark();
		
		//Scenario 5
		System.out.println("##################### Running scenario 5 #####################");
		titanTimes[4] = titanMassiveInsertionBenchmark();
		neo4jTimes[4] = neo4jMassiveInsertionBenchmark();
		orientTimes[4] = orientMassiveInsertionBenchmark();
		
		//Scenario 6
		System.out.println("##################### Running scenario 6 #####################");
		titanTimes[5] = titanMassiveInsertionBenchmark();
		orientTimes[5] = orientMassiveInsertionBenchmark();
		neo4jTimes[5] = neo4jMassiveInsertionBenchmark();
		
		Utils utils = new Utils();
		
		double meanOrientTime = utils.calculateMean(orientTimes);
		double meanTitanTime = utils.calculateMean(titanTimes);
		double meanNeo4jTime = utils.calculateMean(neo4jTimes);
		double varOrientTime = utils.calculateVariance(meanOrientTime, orientTimes);
		double varTitanTime = utils.calculateVariance(meanTitanTime, titanTimes);
		double varNeo4jTime = utils.calculateVariance(meanNeo4jTime, neo4jTimes);
		double stdOrientTime = utils.calculateStdDeviation(varOrientTime);
		double stdTitanTime = utils.calculateStdDeviation(varTitanTime);
		double stdNeo4jTime = utils.calculateStdDeviation(varNeo4jTime);
		
		System.out.println("###########################################################");
		System.out.println("############ Massive Insertion Benchmark Finished ############");
		System.out.println("######################### RESULTS #########################");
		System.out.println("Orient mean execution time: "+meanOrientTime+" milliseconds");
		System.out.println("Orient std execution time: "+stdOrientTime);
		System.out.println("Titan mean execution time: "+meanTitanTime+" milliseconds");
		System.out.println("Titan std execution time: "+stdTitanTime);
		System.out.println("Neo4j mean execution time: "+meanNeo4jTime+" milliseconds");
		System.out.println("Neo4j std execution time: "+stdNeo4jTime);
		System.out.println("###########################################################");
		
	}
	
	public double orientMassiveInsertionBenchmark() {
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		orientGraphDatabase.createGraphForMassiveLoad(orientDBDir);
		long start = System.currentTimeMillis();
		orientGraphDatabase.massiveModeLoading(datasetDir);
		long orientTime = System.currentTimeMillis() - start;
		orientGraphDatabase.shutdownMassiveGraph();
		//wait for some time before try to delete files
		try {
			Thread.sleep(6000);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		Utils utils = new Utils();
		utils.deleteRecursively(new File(orientDBDir));
		return orientTime/1000.0;
	}
	
	public double titanMassiveInsertionBenchmark() {
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		titanGraphDatabase.createGraphForMassiveLoad(titanDBDir);
		titanGraphDatabase.massiveModeLoading(datasetDir);
		long start = System.currentTimeMillis();
		titanGraphDatabase.shutdownMassiveGraph();
		long titanTime = System.currentTimeMillis() - start;
//		wait for some time before try to delete files
		try {
			Thread.sleep(6000);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		Utils utils = new Utils();
		utils.deleteRecursively(new File(titanDBDir));
		return titanTime/1000.0;
	}
	
	public double neo4jMassiveInsertionBenchmark() {
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.createGraphForMassiveLoad(neo4jDBDir);
		neo4jGraphDatabase.massiveModeLoading(datasetDir);
		long start = System.currentTimeMillis();
		neo4jGraphDatabase.shutdownMassiveGraph();
		long neo4jTime = System.currentTimeMillis() - start;
//		wait for some time before try to delete files
		try {
			Thread.sleep(6000);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		Utils utils = new Utils();
		utils.deleteRecursively(new File(neo4jDBDir));
		return neo4jTime/1000.0;
	}
	
}
