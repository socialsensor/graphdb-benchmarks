package eu.socialsensor.singleInsertion;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanMultiVertexQuery;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.query.MultiVertexCentricQueryBuilder;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class TitanSingleInsertion {

	private TitanGraph titanGraph = null;
	
	public static void main(String args[]) {
		TitanSingleInsertion test = new TitanSingleInsertion();
		test.startup("data/titanDB");
		long start = System.currentTimeMillis();
		test.createGraph("data/flickrEdges.txt");
		long duration = System.currentTimeMillis() - start;
		System.out.println("TIME=======> "+duration);
		test.shutdown();
	}
	
	public void startup(String titanDBDir) {
		System.out.println("The Titan database is now starting . . . .");
		BaseConfiguration config = new BaseConfiguration();
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "local");
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, titanDBDir);
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL_KEY, false);
		titanGraph = TitanFactory.open(config);
		titanGraph.makeKey("nodeId").dataType(String.class).indexed(Vertex.class).make();
		titanGraph.makeLabel("similar").unidirected().make();
		titanGraph.commit();
	}
	
	public void shutdown() {
		System.out.println("The Titan database is now shuting down . . . .");
		if(titanGraph != null) {
			titanGraph.shutdown();
			titanGraph = null;
		}
	}
	
	public List<Double> createGraph(String datasetDir) {
		System.out.println("Incrementally creating the Titan database . . . .");
		List<Double> insertionTimes = new ArrayList<Double>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
			String line;
			int lineCounter = 1;
			int nodesCounter = 0;
			long start = System.currentTimeMillis();
			long duration;
			int blocksCounter = 0;
			int nodes = 0;
			Vertex srcVertex, dstVertex;
			while((line = reader.readLine()) != null) {
				if(lineCounter > 4) {
					String[] parts = line.split("\t");
					
					if(titanGraph.query().has("nodeId", Compare.EQUAL, parts[0]).vertices().iterator().hasNext()) {
						srcVertex = titanGraph.query().has("nodeId", Compare.EQUAL, parts[0]).vertices().iterator().next();
					}
					else {
						srcVertex = titanGraph.addVertex(parts[0]);
						titanGraph.commit();
						srcVertex.setProperty("nodeId", parts[0]);
						nodesCounter++;
						nodes++;
					}
					
					if(nodesCounter == 1000) {
						duration = System.currentTimeMillis() - start;
						insertionTimes.add((double) duration);
						nodesCounter = 0;
						start = System.currentTimeMillis();
						
						blocksCounter++;
						System.out.println("Nodes: "+nodes);
						System.out.println("Time: "+duration);
						System.out.println("blocks: "+blocksCounter);
					}
					
					if(titanGraph.query().has("nodeId", Compare.EQUAL, parts[1]).vertices().iterator().hasNext()) {
						dstVertex = titanGraph.query().has("nodeId", Compare.EQUAL, parts[1]).vertices().iterator().next();
					}
					else {
						dstVertex = titanGraph.addVertex(parts[1]);
						titanGraph.commit();
						dstVertex.setProperty("nodeId", parts[1]);
						nodesCounter++;
						nodes++;
					}
					
					Edge edge = titanGraph.addEdge(null, srcVertex, dstVertex, "similar");
					titanGraph.commit();
					
					if(nodesCounter == 1000) {
						duration = System.currentTimeMillis() - start;
						insertionTimes.add((double) duration);
						nodesCounter = 0;
						start = System.currentTimeMillis();
						
						blocksCounter++;
						System.out.println("Nodes: "+nodes);
						System.out.println("Time: "+duration);
						System.out.println("blocks: "+blocksCounter);
					}
	
				}
				lineCounter++;
			}
			duration = System.currentTimeMillis() - start;
			insertionTimes.add((double) duration);
			blocksCounter++;
			System.out.println("Nodes: "+nodesCounter);
			System.out.println("Time: "+duration);
			System.out.println("blocks: "+blocksCounter);
			
			reader.close();
		}
		catch(IOException ioe) {
			ioe.printStackTrace();
		}
		catch( Exception e ) {
			titanGraph.rollback();
		}
		return insertionTimes;
	}

}
