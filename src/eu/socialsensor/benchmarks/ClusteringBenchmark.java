package eu.socialsensor.benchmarks;

import org.apache.log4j.Logger;


public class ClusteringBenchmark {
	
	private final static String orientDBDir = "data/OrientDB";
	private final static String titanDBDir = "data/TitanDB";
	private final static String neo4jDBDir = "data/Neo4jDB";
	
	private Logger logger = Logger.getLogger(ClusteringBenchmark.class);
	
	public void startBenchmark() {
		
		logger.info("Starting CLustering Benchmark");
		
		double[] orientTimes = new double[6];
		double[] titanTimes = new double[6];
		double[] neo4jTimes = new double[6];
		
		//Scenario 1
		
		
	}
	
	
	
}
