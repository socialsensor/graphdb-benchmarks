package eu.socialsensor.benchmarks;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.main.GraphDatabaseBenchmark;
import eu.socialsensor.utils.Utils;

public class MassiveInsertionBenchmark {
		
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
		orientGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.ORIENTDB_PATH);
		long start = System.currentTimeMillis();
		orientGraphDatabase.massiveModeLoading(datasetDir);
		long orientTime = System.currentTimeMillis() - start;
		orientGraphDatabase.shutdownMassiveGraph();
		orientGraphDatabase.delete(GraphDatabaseBenchmark.ORIENTDB_PATH);
		return orientTime/1000.0;
	}
	
	public double titanMassiveInsertionBenchmark() {
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		titanGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.TITANDB_PATH);
		long start = System.currentTimeMillis();
		titanGraphDatabase.massiveModeLoading(datasetDir);
		long titanTime = System.currentTimeMillis() - start;
		titanGraphDatabase.shutdownMassiveGraph();
		titanGraphDatabase.delete(GraphDatabaseBenchmark.TITANDB_PATH);
		return titanTime/1000.0;
	}
	
	public double neo4jMassiveInsertionBenchmark() {
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.NEO4JDB_PATH);
		long start = System.currentTimeMillis();
		neo4jGraphDatabase.massiveModeLoading(datasetDir);
		long neo4jTime = System.currentTimeMillis() - start;
		neo4jGraphDatabase.shutdownMassiveGraph();
		neo4jGraphDatabase.delete(GraphDatabaseBenchmark.NEO4JDB_PATH);
		return neo4jTime/1000.0;
	}
	
}
