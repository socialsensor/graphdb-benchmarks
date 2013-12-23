package eu.socialsensor.benchmarks;

import java.io.File;
import java.util.List;

import eu.socialsensor.singleInsertion.Neo4jSingleInsertion;
import eu.socialsensor.singleInsertion.OrientSingleInsertion;
import eu.socialsensor.singleInsertion.TitanSingleInsertion;
import eu.socialsensor.main.Utils;

public class SingleInsertionBenchmark {
	
	private final static String orientDBDir = "data/OrientDB";
	private final static String titanDBDir = "data/TitanDB";
	private final static String neo4jDBDir = "data/Neo4jDB";
	
	private String datasetDir;
	
	public SingleInsertionBenchmark(String datasetDir) {
		this.datasetDir = datasetDir;
	}
	
	public void startBenchmark() {
		
		titanSingleInsertionBenchmark();
//		orientSingleInsertionBenchmark();
//		neo4jSinglesInsertionBenchmark();
		
	}
	
	public void titanSingleInsertionBenchmark() {
		TitanSingleInsertion titanSingleInsertion = new TitanSingleInsertion();
		titanSingleInsertion.startup(titanDBDir);
		List<Long> titanInsertionTimes = titanSingleInsertion.createGraph(datasetDir);
		titanSingleInsertion.shutdown();
		try {
			Thread.sleep(60000);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		Utils utils = new Utils();
		utils.writeTimes(titanInsertionTimes, "data/titanInsertionTimes");
		utils.deleteRecursively(new File(titanDBDir));
	}
		
	public void orientSingleInsertionBenchmark() {
		OrientSingleInsertion orientSingleInsertion = new OrientSingleInsertion();
		orientSingleInsertion.strartup(orientDBDir);
		List<Long> orientInsertionTimes = orientSingleInsertion.createGraph(datasetDir);
		orientSingleInsertion.shutdown();
		try {
			Thread.sleep(60000);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		Utils utils = new Utils();
		utils.writeTimes(orientInsertionTimes, "data/orientInsertionTimes");
		utils.deleteRecursively(new File(orientDBDir));
	}
	
	public void neo4jSinglesInsertionBenchmark() {
		Neo4jSingleInsertion neo4jSingleInsertion = new Neo4jSingleInsertion();
		neo4jSingleInsertion.startup(neo4jDBDir);
		List<Long> neo4jInsertionTimes = neo4jSingleInsertion.createGraph(datasetDir);
		neo4jSingleInsertion.shutdown();
		try {
			Thread.sleep(60000);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		Utils utils = new Utils();
		utils.writeTimes(neo4jInsertionTimes, "data/neo4jInsertionTimes");
		utils.deleteRecursively(new File(neo4jDBDir));
	}

}
