package eu.socialsensor.benchmarks;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.query.Neo4jQuery;
import eu.socialsensor.query.OrientQuery;
import eu.socialsensor.query.TitanQuery;
import eu.socialsensor.utils.Utils;

public class FindShortestPathBenchmark {
	
	private final static String orientDBDir = "data/OrientDB";
	private final static String titanDBDir = "data/TitanDB";
	private final static String neo4jDBDir = "data/Neo4jDB";
		
	public void startBenchmark() {
		
		System.out.println("###########################################################");
		System.out.println("############ Starting Find Shortest Path Benchmark ############");
		System.out.println("###########################################################");
		
		double[] orientTimes = new double[6];
		double[] titanTimes = new double[6];
		double[] neo4jTimes = new double[6];
		
		//Scenario 1
		orientTimes[0] = orientFindShortestPathBenchmark();
		titanTimes[0] = titanFindShortestPathBenchmark();
		neo4jTimes[0] = neo4jFindShortestPathBenchmark();
		
		//Scenario 2
		orientTimes[1] = orientFindShortestPathBenchmark();
		neo4jTimes[1] = neo4jFindShortestPathBenchmark();
		titanTimes[1] = titanFindShortestPathBenchmark();
		
		//Scenario 3
		neo4jTimes[2] = neo4jFindShortestPathBenchmark();
		orientTimes[2] = orientFindShortestPathBenchmark();
		titanTimes[2] = titanFindShortestPathBenchmark();
		
		//Scenario 4
		neo4jTimes[3] = neo4jFindShortestPathBenchmark();
		titanTimes[3] = titanFindShortestPathBenchmark();
		orientTimes[3] = orientFindShortestPathBenchmark();
		
		//Scenario 5
		titanTimes[4] = titanFindShortestPathBenchmark();
		neo4jTimes[4] = neo4jFindShortestPathBenchmark();
		orientTimes[4] = orientFindShortestPathBenchmark();
		
		//Scenario 6
		titanTimes[5] = titanFindShortestPathBenchmark();
		orientTimes[5] = orientFindShortestPathBenchmark();
		neo4jTimes[5] = neo4jFindShortestPathBenchmark();
		
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
		System.out.println("############ Find Shortest Path Benchmark Finished ############");
		System.out.println("######################### RESULTS #########################");
		System.out.println("Orient mean execution time: "+meanOrientTime+" nanoseconds");
		System.out.println("Orient std execution time: "+stdOrientTime);
		System.out.println("Titan mean execution time: "+meanTitanTime+" nanoseconds");
		System.out.println("Titan std execution time: "+stdTitanTime);
		System.out.println("Neo4j mean execution time: "+meanNeo4jTime+" nanoseconds");
		System.out.println("Neo4j std execution time: "+stdNeo4jTime);
		System.out.println("###########################################################");
		
	}
	
	public double orientFindShortestPathBenchmark() {
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		orientGraphDatabase.open(orientDBDir);
		long start = System.currentTimeMillis();
		orientGraphDatabase.shorestPathQuery();
		long orientTime = System.currentTimeMillis() - start;
		orientGraphDatabase.shutdown();
		return orientTime/1000.0;	}
	
	public double titanFindShortestPathBenchmark() {
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		titanGraphDatabase.open(titanDBDir);
		long start = System.currentTimeMillis();
		titanGraphDatabase.shorestPathQuery();
		long titanTime = System.currentTimeMillis() - start;
		titanGraphDatabase.shutdown();
		return titanTime/1000.0;
	}
	
	public double neo4jFindShortestPathBenchmark() {
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.open(neo4jDBDir);
		long start = System.currentTimeMillis();
		neo4jGraphDatabase.neighborsOfAllNodesQuery();
		long neo4jTime = System.currentTimeMillis() - start;
		neo4jGraphDatabase.shutdown();
		return neo4jTime/1000.0;	}

}
