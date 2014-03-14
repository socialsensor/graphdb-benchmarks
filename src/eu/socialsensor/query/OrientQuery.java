package eu.socialsensor.query;

import java.util.Iterator;

import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.gremlin.java.GremlinPipeline;

public class OrientQuery  implements Query {
	
	private OrientGraph orientGraph = null;
	
//	public static void main(String args[]) {
//		OrientQuery test = new OrientQuery();
//		test.openDB("data/orientDB");
//		test.findShortestPath();
//		test.shutdown();
//	}
	
//	public void openDB(String orientDBDir) {
//		System.out.println("The Orient database is now opening . . . .");
//		orientGraph = new OrientGraph("plocal:"+orientDBDir);	
//	}
//	
//	public void shutdown() {
//		System.out.println("The Orient database is now shutting down . . . .");
//		orientGraph.shutdown();
//		orientGraph = null;
//	}
	
	public OrientQuery(OrientGraph orientGraph) {
		this.orientGraph = orientGraph;
	}
	
	public void findNeighborsOfAllNodes() {
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
	
	public void findShortestPaths() {
		Iterable<Vertex> vertices = orientGraph.getVertices();
		Iterator<Vertex> vertexIter = vertices.iterator();
		Vertex v1 = vertexIter.next();
		int iterations = 0;
		while(iterations < 5) {
			Vertex v2 = vertexIter.next();
			double start = System.currentTimeMillis() / 1000.0;
			Iterable<Object> spath = orientGraph.getRawGraph().command(new OSQLSynchQuery<Object>( 
					"select shortestPath("+v1.getId()+","+v2.getId()+",'BOTH')"));
			System.out.println(spath.iterator().next());
			double end = System.currentTimeMillis() / 1000.0;
			System.out.println(end - start);
			iterations++;
		}
	}
}
