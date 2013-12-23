package eu.socialsensor.query;

//import java.util.Iterator;
//import java.util.List;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;

import java.util.Iterator;
import java.util.List;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.graph.sql.functions.OSQLFunctionDijkstra;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
//import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe.LoopBundle;


public class OrientQuery {
	
	private OrientGraph orientGraph = null;
	
	public static void main(String args[]) {
		OrientQuery test = new OrientQuery();
		test.openDB("data/orientDB");
		test.findShortestPath();
		test.shutdown();
	}
	
	public void openDB(String orientDBDir) {
		System.out.println("The Orient database is now opening . . . .");
		orientGraph = new OrientGraph("plocal:"+orientDBDir);
	}
	
	public void shutdown() {
		System.out.println("The Orient database is now shutting down . . . .");
		orientGraph.shutdown();
		orientGraph = null;
	}
	
	public void findNeighboursOfAllNodes() {
		for(Vertex v : orientGraph.getVertices()) {
			GremlinPipeline<String, Vertex> getNeighboursPipe = new GremlinPipeline<String, Vertex>(v).both("similar");
		}
	}
	
	public void findNodesOfAllEdges() {
		for(Vertex v : orientGraph.getVertices()) {
			for(Edge e : v.getEdges(Direction.BOTH)) {
				GremlinPipeline<String, Vertex> getNodesPipe = new GremlinPipeline<String, Vertex>(e).bothV();
			}
		}
	}
	
	public void findShortestPath() {
//		Iterable<Vertex> vertices = orientGraph.getVertices();
//		Iterator<Vertex> vertexIter = vertices.iterator();
		Vertex v1 = null;
		int iteration = 0;
		
		for(Vertex v : orientGraph.getVertices()) {
			if(iteration == 0) {
				v1 = v;
			}
			else {
				final Vertex v2 = v;

				GremlinPipeline<String, List> pathPipe = new GremlinPipeline<String, List>(v1)
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
				if(iteration == 10)
					break;
			}
			iteration++;
		}
	}
}
