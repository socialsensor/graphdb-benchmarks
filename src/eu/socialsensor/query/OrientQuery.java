package eu.socialsensor.query;

import java.util.Iterator;
import java.util.List;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe.LoopBundle;

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
			final Vertex v2 = orientGraph.getVertices("nodeId", String.valueOf(i)).iterator().next();
			@SuppressWarnings("rawtypes")
			final GremlinPipeline<String, List> pathPipe = new GremlinPipeline<String, List>(v1)
					.as("similar")
					.out("similar")
					.loop("similar", new PipeFunction<LoopBundle<Vertex>, Boolean>() {
						//@Override
						public Boolean compute(LoopBundle<Vertex> bundle) {
							return bundle.getLoops() < 5 && !bundle.getObject().equals(v2);
						}
					})
					.path();
			@SuppressWarnings("unused")
			int length = pathPipe.iterator().next().size();
		}
		
	}
}
