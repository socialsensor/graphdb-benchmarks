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
import eu.socialsensor.graphdatabases.SparkseeGraphDatabase;
import eu.socialsensor.utils.Utils;

public class SparkseeSingleInsertion implements Insertion {
	
	public static String INSERTION_TIMES_OUTPUT_PATH = null;
	
	private static int count;
	
	Session session = null;
	Graph sparkseeGraph = null;
	
	Value value = new Value();
	
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
			long start = System.currentTimeMillis();
			long duration;
			long srcNode, dstNode;
			
			while((line = reader.readLine()) != null) {
				if(lineCounter > 4) {
					String[] parts = line.split("\t");
					
					srcNode = sparkseeGraph.findOrCreateObject(SparkseeGraphDatabase.NODE_ATTRIBUTE, 
							value.setString(parts[0]));
					dstNode = sparkseeGraph.findOrCreateObject(SparkseeGraphDatabase.NODE_ATTRIBUTE, 
							value.setString(parts[1]));
					
					session.begin();
					sparkseeGraph.newEdge(SparkseeGraphDatabase.EDGE_TYPE, srcNode, dstNode);
					session.commit();
					
					if(lineCounter % 1000 ==0) {
						duration = System.currentTimeMillis() - start;
						insertionTimes.add((double) duration);
						start = System.currentTimeMillis();
					}					
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
