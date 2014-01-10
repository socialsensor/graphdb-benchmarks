package eu.socialsensor.benchmarks;

import eu.socialsensor.query.Neo4jQuery;
import eu.socialsensor.query.OrientQuery;
import eu.socialsensor.query.TitanQuery;
import eu.socialsensor.utils.Utils;

public class FindNodesOfAllEdgesBenchmark {
	
	private final static String orientDBDir = "data/OrientDB";
	private final static String titanDBDir = "data/TitanDB";
	private final static String neo4jDBDir = "data/Neo4jDB";

	public void startBenchmark() {
		System.out.println("###########################################################");
		System.out.println("############ Starting Find Nodes of Each Edge Benchmark ############");
		System.out.println("###########################################################");
		
		double[] orientTimes = new double[6];
		double[] titanTimes = new double[6];
		double[] neo4jTimes = new double[6];
		
		//Scenario 1
		orientTimes[0] = orientFindNodesOfAllEdgesBenchmark();
		titanTimes[0] = titanFindNodesOfAllEdgesBenchmark();
		neo4jTimes[0] = neo4jFindNodesOfAllEdgesBenchmark();
		
		//Scenario 2
		orientTimes[1] = orientFindNodesOfAllEdgesBenchmark();
		neo4jTimes[1] = neo4jFindNodesOfAllEdgesBenchmark();
		titanTimes[1] = titanFindNodesOfAllEdgesBenchmark();
		
		//Scenario 3
		neo4jTimes[2] = neo4jFindNodesOfAllEdgesBenchmark();
		orientTimes[2] = orientFindNodesOfAllEdgesBenchmark();
		titanTimes[2] = titanFindNodesOfAllEdgesBenchmark();
		
		//Scenario 4
		neo4jTimes[3] = neo4jFindNodesOfAllEdgesBenchmark();
		titanTimes[3] = titanFindNodesOfAllEdgesBenchmark();
		orientTimes[3] = orientFindNodesOfAllEdgesBenchmark();
		
		//Scenario 5
		titanTimes[4] = titanFindNodesOfAllEdgesBenchmark();
		neo4jTimes[4] = neo4jFindNodesOfAllEdgesBenchmark();
		orientTimes[4] = orientFindNodesOfAllEdgesBenchmark();
		
		//Scenario 6
		titanTimes[5] = titanFindNodesOfAllEdgesBenchmark();
		orientTimes[5] = orientFindNodesOfAllEdgesBenchmark();
		neo4jTimes[5] = neo4jFindNodesOfAllEdgesBenchmark();
		
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
		System.out.println("############ Find Nodes of Each Edge Benchmark Finished ############");
		System.out.println("######################### RESULTS #########################");
		System.out.println("Orient mean execution time: "+meanOrientTime+" nanoseconds");
		System.out.println("Orient std execution time: "+stdOrientTime);
		System.out.println("Titan mean execution time: "+meanTitanTime+" nanoseconds");
		System.out.println("Titan std execution time: "+stdTitanTime);
		System.out.println("Neo4j mean execution time: "+meanNeo4jTime+" nanoseconds");
		System.out.println("Neo4j std execution time: "+stdNeo4jTime);
		System.out.println("###########################################################");
		
	}
	
	public double orientFindNodesOfAllEdgesBenchmark() {
		OrientQuery orientQuery = new OrientQuery();
		orientQuery.openDB(orientDBDir);;
		long start = System.currentTimeMillis();
		orientQuery.findNodesOfAllEdges();
		long orientTime = System.currentTimeMillis() - start;
		orientQuery.shutdown();
		return orientTime/1000.0;
	}
	
	public double titanFindNodesOfAllEdgesBenchmark() {
		TitanQuery titanQuery = new TitanQuery();
		titanQuery.openDB(titanDBDir);
		long start = System.currentTimeMillis();
		titanQuery.findNodesOfAllEdges();
		long titanTime = System.currentTimeMillis() - start;
		titanQuery.shutdown();
		return titanTime/1000.0;
	}
	
	public double neo4jFindNodesOfAllEdgesBenchmark() {
		Neo4jQuery neo4jQuery = new Neo4jQuery();
		neo4jQuery.openDB(neo4jDBDir);
		long start = System.currentTimeMillis();
		neo4jQuery.findNodesOfAllEdges();
		long neo4jTime = System.currentTimeMillis() - start;
		neo4jQuery.shutdown();
		return neo4jTime/1000.0;
	}

}
