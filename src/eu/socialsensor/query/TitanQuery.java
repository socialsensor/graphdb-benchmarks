package eu.socialsensor.query;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe.LoopBundle;

public class TitanQuery {
	
	private TitanGraph titanGraph = null;
	
	public static void main(String args[]) {
		TitanQuery test = new TitanQuery();
		test.openDB("data/titanDB");
//		test.findNodesOfAllEdges();
//		test.findNeighboursOfAllNodes();
		test.findShortestPath();
		test.shutdown();
		
	}
	
	public void shutdown() {
		System.out.println("The Titan database is now shuting down . . . .");
		if(titanGraph != null) {
			titanGraph.shutdown();
			titanGraph = null;
		}
	}
	
	public void openDB(String titanDBDir) {
		System.out.println("The Titan database is now starting . . . .");
		BaseConfiguration config = new BaseConfiguration();
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "local");
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, titanDBDir);
		titanGraph = TitanFactory.open(config);
	}
	
	public void findNeighboursOfAllNodes() {
		for(Vertex v : titanGraph.getVertices()) {
			GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(v).both("similar");
		}
	}
	
	public void findNodesOfAllEdges() {
		for(Edge e : titanGraph.getEdges()) {
			GremlinPipeline<String, Vertex> getNodesPipe = new GremlinPipeline<String, Vertex>(e).bothV();
		}
	}
	
	public void findShortestPath() {
		Iterable<Vertex> vertices = titanGraph.getVertices();
		Iterator<Vertex> vertexIter = vertices.iterator();
		final Vertex v1 = vertexIter.next();
		int iterations = 0;
		while(iterations < 5) {
			final Vertex v2 = vertexIter.next();
			final GremlinPipeline<String, List> pathPipe = new GremlinPipeline<String, List>(v1)
					.as("similar")
					.both("similar")
					.loop("similar", new PipeFunction<LoopBundle<Vertex>, Boolean>() {
						//@Override
						public Boolean compute(LoopBundle<Vertex> bundle) {
							return bundle.getLoops() < 5 && bundle.getObject() != v2;
						}
					})
					.path();
			int length = pathPipe.iterator().next().size() - 1;
			System.out.println(v1.getProperty("nodeId")+" and "+v2.getProperty("nodeId")+" has length: "+length);
			iterations++;
		}
	}
	
}
