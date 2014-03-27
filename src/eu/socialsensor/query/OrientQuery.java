package eu.socialsensor.query;

import java.util.Iterator;

import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.gremlin.java.GremlinPipeline;

import eu.socialsensor.benchmarks.FindShortestPathBenchmark;

public class OrientQuery  implements Query {
	
	private OrientGraph orientGraph = null;
	
	public static void main(String args[]) {
	}
	
	
	public OrientQuery(OrientGraph orientGraph) {
		this.orientGraph = orientGraph;
	}
	
	public void findNeighborsOfAllNodes() {
		for(Vertex v : orientGraph.getVertices()) {
			@SuppressWarnings("unused")
			GremlinPipeline<String, Vertex> getNeighboursPipe = new GremlinPipeline<String, Vertex>(v).both("similar");
		}
	}
	
	public void findNodesOfAllEdges() {
		for(Vertex v : orientGraph.getVertices()) {
			for(Edge e : v.getEdges(Direction.BOTH)) {
				GremlinPipeline<String, Vertex> getNodesPipe = new GremlinPipeline<String, Vertex>(e).bothV();
				Iterator<Vertex> vertexIter = getNodesPipe.iterator();
				@SuppressWarnings("unused")
				Vertex startNode = vertexIter.next();
				@SuppressWarnings("unused")
				Vertex endNode = vertexIter.next();
			}
		}
	}
	
	public void findShortestPaths() {
		Vertex v1 = orientGraph.getVertices("nodeId", "1").iterator().next();
		for(int i : FindShortestPathBenchmark.generatedNodes) {
			Vertex v2 = orientGraph.getVertices("nodeId", String.valueOf(i)).iterator().next();
			Iterable<Object> spath = orientGraph.getRawGraph().command(new OSQLSynchQuery<Object>( 
					"select shortestPath("+v1.getId()+","+v2.getId()+",'OUT')"));
			@SuppressWarnings("unused")
			Object length = spath.iterator().next();
		}
		
	}
}
