package eu.socialsensor.benchmarks;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.main.GraphDatabaseBenchmark;
import eu.socialsensor.utils.Utils;

/**
 * FindNeighboursOfAllNodesBenchmark implementation
 * @author sotbeis
 * @email sotbeis@iti.gr
 */
public class FindNeighboursOfAllNodesBenchmark implements Benchmark {
	
	@Override
	public void startBenchmark() {
		System.out.println("#########################################################################");
		System.out.println("############ Starting Find Neighbours of Each Node Benchmark ############");
		System.out.println("#########################################################################");
		
		double[] orientTimes = new double[6];
		double[] titanTimes = new double[6];
		double[] neo4jTimes = new double[6];
		
		//Scenario 1
		orientTimes[0] = orientFindNeighboursOfAllNodesBenchmark();
		titanTimes[0] = titanFindNeighboursOfAllNodesBenchmark();
		neo4jTimes[0] = neo4jFindNeighboursOfAllNodesBenchmark();
		
		//Scenario 2
		orientTimes[1] = orientFindNeighboursOfAllNodesBenchmark();
		neo4jTimes[1] = neo4jFindNeighboursOfAllNodesBenchmark();
		titanTimes[1] = titanFindNeighboursOfAllNodesBenchmark();
		
		//Scenario 3
		neo4jTimes[2] = neo4jFindNeighboursOfAllNodesBenchmark();
		orientTimes[2] = orientFindNeighboursOfAllNodesBenchmark();
		titanTimes[2] = titanFindNeighboursOfAllNodesBenchmark();
		
		//Scenario 4
		neo4jTimes[3] = neo4jFindNeighboursOfAllNodesBenchmark();
		titanTimes[3] = titanFindNeighboursOfAllNodesBenchmark();
		orientTimes[3] = orientFindNeighboursOfAllNodesBenchmark();
		
		//Scenario 5
		titanTimes[4] = titanFindNeighboursOfAllNodesBenchmark();
		neo4jTimes[4] = neo4jFindNeighboursOfAllNodesBenchmark();
		orientTimes[4] = orientFindNeighboursOfAllNodesBenchmark();
		
		//Scenario 6
		titanTimes[5] = titanFindNeighboursOfAllNodesBenchmark();
		orientTimes[5] = orientFindNeighboursOfAllNodesBenchmark();
		neo4jTimes[5] = neo4jFindNeighboursOfAllNodesBenchmark();
		
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
		
		System.out.println("#########################################################################");
		System.out.println("############ Find Neighbours of Each Node Benchmark Finished ############");
		System.out.println("#########################################################################");
		System.out.println("################################ RESULTS ################################");
		System.out.println("Orient mean execution time: "+meanOrientTime+" nanoseconds");
		System.out.println("Orient std execution time: "+stdOrientTime);
		System.out.println("Titan mean execution time: "+meanTitanTime+" nanoseconds");
		System.out.println("Titan std execution time: "+stdTitanTime);
		System.out.println("Neo4j mean execution time: "+meanNeo4jTime+" nanoseconds");
		System.out.println("Neo4j std execution time: "+stdNeo4jTime);
		System.out.println("#########################################################################");
		
	}
	
	private double orientFindNeighboursOfAllNodesBenchmark() {
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		orientGraphDatabase.open(GraphDatabaseBenchmark.ORIENTDB_PATH);
		long start = System.currentTimeMillis();
		orientGraphDatabase.neighborsOfAllNodesQuery();
		long orientTime = System.currentTimeMillis() - start;
		orientGraphDatabase.shutdown();
		return orientTime/1000.0;
	}
	
	private double titanFindNeighboursOfAllNodesBenchmark() {
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		titanGraphDatabase.open(GraphDatabaseBenchmark.TITANDB_PATH);
		long start = System.currentTimeMillis();
		titanGraphDatabase.neighborsOfAllNodesQuery();
		long titanTime = System.currentTimeMillis() - start;
		titanGraphDatabase.shutdown();
		return titanTime/1000.0;
	}
	
	private double neo4jFindNeighboursOfAllNodesBenchmark() {
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.open(GraphDatabaseBenchmark.NEO4JDB_PATH);
		long start = System.currentTimeMillis();
		neo4jGraphDatabase.neighborsOfAllNodesQuery();
		long neo4jTime = System.currentTimeMillis() - start;
		neo4jGraphDatabase.shutdown();
		return neo4jTime/1000.0;
	}

}
