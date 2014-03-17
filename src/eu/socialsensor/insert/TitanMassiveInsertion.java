package eu.socialsensor.insert;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.VertexIDType;

public class TitanMassiveInsertion implements Insertion {
	
//	private TitanGraph titanGraph = null;
	private BatchGraph<TitanGraph> batchGraph = null;
	
	Logger logger = Logger.getLogger(TitanMassiveInsertion.class);
	
	public TitanMassiveInsertion(BatchGraph<TitanGraph> batchGraph) {
		this.batchGraph = batchGraph;
	}
	
	
	public void createGraph(String datasetDir) {
		System.out.println("Loading data in massive mode in Titan database");
//		logger.info("Loading data in massive mode in Titan database");
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
			String line;
			int lineCounter = 1;
			Vertex srcVertex, dstVertex;
			while((line = reader.readLine()) != null) {
//				if(lineCounter > 4) {
					String[] parts = line.split("\t");
					
					srcVertex = getOrCreate(parts[0]);
					dstVertex = getOrCreate(parts[1]);
					
					srcVertex.addEdge("similar", dstVertex);
				}
				lineCounter++;
//			}
			reader.close();
		}
		catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	private Vertex getOrCreate(String value) {
		Vertex vertex = batchGraph.getVertex(value);
		if(vertex == null) {
			vertex = batchGraph.addVertex(value);
			vertex.setProperty("nodeId", value);
		}
		return vertex;
		
	}


}
