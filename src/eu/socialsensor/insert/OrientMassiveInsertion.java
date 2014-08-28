package eu.socialsensor.insert;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Implementation of massive Insertion in OrientDB
 * graph database
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 * 
 */
public class OrientMassiveInsertion implements Insertion {
	
	private OrientGraphNoTx orientGraph = null;
	Index<Vertex> vetrices = null;
	
	private Logger logger = Logger.getLogger(OrientMassiveInsertion.class);
	
	public OrientMassiveInsertion(OrientGraphNoTx orientGraph, Index<Vertex> vertices) {
		this.orientGraph = orientGraph;
		this.vetrices = vertices;
	}
	
	@Override
	public void createGraph(String datasetDir) {
		logger.setLevel(Level.INFO);
		logger.info("Loading data in massive mode in OrientDB database . . . .");
		orientGraph.getRawGraph().declareIntent(new OIntentMassiveInsert());
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
			String line;
			int lineCounter = 1;
			Vertex srcVertex, dstVertex;
			Iterable<Vertex> cache;
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
					}

					cache = vetrices.get("nodeId", parts[1]);
					if(cache.iterator().hasNext()) {
						dstVertex = cache.iterator().next();
					}
					else {
						dstVertex = orientGraph.addVertex(null, "nodeId", parts[1] );
						vetrices.put("nodeId", parts[1], dstVertex);
					}
					
					if(parts[0].equals(parts[1])) {
						dstVertex = srcVertex;
					}
					
					orientGraph.addEdge(null, srcVertex, dstVertex, "similar");
				}
				lineCounter++;
			}			
			reader.close();
			orientGraph.getRawGraph().declareIntent(null);
		}
		catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

}
