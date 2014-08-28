package eu.socialsensor.insert;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import eu.socialsensor.benchmarks.SingleInsertionBenchmark;
import eu.socialsensor.utils.Utils;

/**
 * Implementation of single Insertion in OrientDB
 * graph database
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 * 
 */
public class OrientSingleInsertion implements Insertion {
	
	public static String INSERTION_TIMES_OUTPUT_PATH = null;

	private static int count;
	
	private OrientGraph orientGraph = null;
	Index<Vertex> vetrices = null;
	
	private Logger logger = Logger.getLogger(OrientSingleInsertion.class);
	
	public OrientSingleInsertion(OrientGraph orientGraph, Index<Vertex> vertices) {
		this.orientGraph = orientGraph;
		this.vetrices = vertices;
	}
	
	@Override
	public void createGraph(String datasetDir) {
		INSERTION_TIMES_OUTPUT_PATH = SingleInsertionBenchmark.INSERTION_TIMES_OUTPUT_PATH + ".orient";
		logger.setLevel(Level.INFO);
		count++;
		logger.info("Incrementally loading data in Orient database . . . .");
		List<Double> insertionTimes = new ArrayList<Double>(); 
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
			String line;
			int lineCounter = 1;
			int nodesCounter = 0;
			Vertex srcVertex, dstVertex;
			Iterable<Vertex> cache;
			long start = System.currentTimeMillis();
			long duration;
			while((line = reader.readLine()) != null) {
				if(lineCounter > 4) {
					String[] parts = line.split("\t");
					
					cache = vetrices.get("nodeId", parts[0]);
					if(cache.iterator().hasNext()) {
						srcVertex = cache.iterator().next();
					}
					else {
						srcVertex = orientGraph.addVertex(null, "nodeId", parts[0] );
						vetrices.put("nodeId", parts[0], srcVertex);
						orientGraph.commit();
						nodesCounter++;
					}
					
					if(nodesCounter == 1000) {
						duration = System.currentTimeMillis() - start;
						insertionTimes.add((double) duration);
						nodesCounter = 0;
						start = System.currentTimeMillis();
					}
					
					cache = vetrices.get("nodeId", parts[1]);
					if(cache.iterator().hasNext()) {
						dstVertex = cache.iterator().next();
					}
					else {
						dstVertex = orientGraph.addVertex(null, "nodeId", parts[1] );
						vetrices.put("nodeId", parts[1], dstVertex);
						orientGraph.commit();
						nodesCounter++;
					}
					
					orientGraph.addEdge(null, srcVertex, dstVertex, "similar");
					orientGraph.commit();
					
					if(nodesCounter == 1000) {
						duration = System.currentTimeMillis() - start;
						insertionTimes.add((double) duration);
						nodesCounter = 0;
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
			System.out.println(ioe);
			ioe.printStackTrace();
		}
		catch( Exception e ) {
			System.out.println(e);
			orientGraph.rollback();
		}
		Utils utils = new Utils();
		utils.writeTimes(insertionTimes, OrientSingleInsertion.INSERTION_TIMES_OUTPUT_PATH+"."+count);
	}
}
