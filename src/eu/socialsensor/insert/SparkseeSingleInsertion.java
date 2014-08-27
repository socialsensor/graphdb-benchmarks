package eu.socialsensor.insert;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.sparsity.sparksee.gdb.Graph;
import com.sparsity.sparksee.gdb.Session;
import com.sparsity.sparksee.gdb.Value;

import eu.socialsensor.benchmarks.SingleInsertionBenchmark;
import eu.socialsensor.utils.Utils;

public class SparkseeSingleInsertion implements Insertion {
	
	public static String INSERTION_TIMES_OUTPUT_PATH = null;
	
	private static int count;
	
	Session session = null;
	Graph sparkseeGraph = null;
	
	private Logger logger = Logger.getLogger(SparkseeSingleInsertion.class);
	
	public SparkseeSingleInsertion(Session session) {
		this.session = session;
		this.sparkseeGraph = session.getGraph();
	}
	
	@Override
	public void createGraph(String datasetDir) {
		INSERTION_TIMES_OUTPUT_PATH = SingleInsertionBenchmark.INSERTION_TIMES_OUTPUT_PATH + ".sparksee";
		logger.setLevel(Level.INFO);
		count++;
		logger.info("Incrementally loading data in Sparksee database . . . .");
		List<Double> insertionTimes = new ArrayList<Double>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
			String line;
			int lineCounter = 1;
			int nodesCounter = 0;
			long start = System.currentTimeMillis();
			long duration;
			long srcNode, dstNode;
			
			int nodeType = sparkseeGraph.findType("node");
			int nodeAttribute = sparkseeGraph.findAttribute(nodeType, "nodeId");
			int edgeType = sparkseeGraph.findType("similar");
			
			Value value = new Value();
			while((line = reader.readLine()) != null) {
				if(lineCounter > 4) {
					String[] parts = line.split("\t");
					
					srcNode = sparkseeGraph.findObject(nodeAttribute, value.setString(parts[0]));
					if(srcNode == 0) {
						session.begin();
						srcNode = sparkseeGraph.newNode(nodeType);
						sparkseeGraph.setAttribute(srcNode, nodeAttribute, value.setString(parts[0]));
						session.commit();
						nodesCounter++;
					}
					
					if(nodesCounter == 1000) {
						duration = System.currentTimeMillis() - start;
						insertionTimes.add((double) duration);
						nodesCounter = 0;
						start = System.currentTimeMillis();
					}
					
					dstNode = sparkseeGraph.findObject(nodeAttribute, value.setString(parts[1]));
					if(dstNode == 0) {
						session.begin();
						dstNode = sparkseeGraph.newNode(nodeType);
						sparkseeGraph.setAttribute(dstNode, nodeAttribute, value.setString(parts[1]));
						session.commit();
						nodesCounter++;
					}
					
					if(nodesCounter == 1000) {
						duration = System.currentTimeMillis() - start;
						insertionTimes.add((double) duration);
						nodesCounter = 0;
						start = System.currentTimeMillis();
					}
					
					session.begin();
					sparkseeGraph.newEdge(edgeType, srcNode, dstNode);
					session.commit();
					
				}
				lineCounter++;
			}
			duration = System.currentTimeMillis() - start;
			insertionTimes.add((double) duration);
			reader.close();
		}
		catch(IOException ioe) {
			ioe.printStackTrace();
		}
		
		Utils utils = new Utils();
		utils.writeTimes(insertionTimes, SparkseeSingleInsertion.INSERTION_TIMES_OUTPUT_PATH+"."+count);
	}
		

}
