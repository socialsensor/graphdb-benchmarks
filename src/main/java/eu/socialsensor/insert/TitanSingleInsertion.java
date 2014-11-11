package eu.socialsensor.insert;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;

import eu.socialsensor.benchmarks.SingleInsertionBenchmark;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.main.GraphDatabaseBenchmark;
import eu.socialsensor.utils.Utils;

/**
 * Implementation of single Insertion in Titan
 * graph database
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 * 
 */
public class TitanSingleInsertion implements Insertion {
	
	public static String INSERTION_TIMES_OUTPUT_PATH = null;
	
	private static int count;
	
	private TitanGraph titanGraph = null;
	
	private Logger logger = Logger.getLogger(TitanSingleInsertion.class);
	
	public static void main(String[] args) {
		GraphDatabase graph = new TitanGraphDatabase();
		graph.createGraphForSingleLoad(GraphDatabaseBenchmark.TITANDB_PATH);
		graph.singleModeLoading("./data/enronEdges.txt");
		graph.shutdown();
		
		graph.delete(GraphDatabaseBenchmark.TITANDB_PATH);
	}
		
	public TitanSingleInsertion(TitanGraph titanGraph) {
		this.titanGraph = titanGraph;
	}
	
	@Override
	public void createGraph(String datasetDir) {
		INSERTION_TIMES_OUTPUT_PATH = SingleInsertionBenchmark.INSERTION_TIMES_OUTPUT_PATH + ".titan";
		logger.setLevel(Level.INFO);
		count++;
		logger.info("Incrementally loading data in Titan database . . . .");
		List<Double> insertionTimes = new ArrayList<Double>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
			String line;
			int lineCounter = 1;
			long start = System.currentTimeMillis();
			long duration;
			Vertex srcVertex, dstVertex;
			while((line = reader.readLine()) != null) {
				if(lineCounter > 4) {
					String[] parts = line.split("\t");
					
					srcVertex = getOrCreate(Integer.valueOf(parts[0]));
					dstVertex = getOrCreate(Integer.valueOf(parts[1]));
					
					titanGraph.addEdge(null, srcVertex, dstVertex, "similar");
					titanGraph.commit();
					
					if(lineCounter % 1000 == 0) {
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
		catch( Exception e ) {
			titanGraph.rollback();
		}
		Utils utils = new Utils();
		utils.writeTimes(insertionTimes, TitanSingleInsertion.INSERTION_TIMES_OUTPUT_PATH+"."+count);
	}
	
	private Vertex getOrCreate(Integer nodeId) {
		Vertex v;
		if(titanGraph.query().has("nodeId", Compare.EQUAL, nodeId).vertices().iterator()
				.hasNext()) {
			v = (Vertex)titanGraph.query().has("nodeId", Compare.EQUAL, nodeId)
					.vertices().iterator().next();
		}
		else {
			v = titanGraph.addVertex(nodeId);
			v.setProperty("nodeId", nodeId);
			titanGraph.commit();
		}
		return v;
	}

}
