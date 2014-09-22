package eu.socialsensor.query;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.graph.sql.functions.OSQLFunctionShortestPath;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientExtendedGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe.LoopBundle;

import eu.socialsensor.benchmarks.FindShortestPathBenchmark;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

/**
 * Query implementation for OrientDB graph database
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 */
public class OrientQuery implements Query {
	private OrientExtendedGraph orientGraph = null;

	public OrientQuery(OrientExtendedGraph orientGraph) {
		this.orientGraph = orientGraph;
	}

	public static void main(String args[]) {
	}

	@Override
	@SuppressWarnings("unused")
	public void findNeighborsOfAllNodes() {
		for (Vertex v : orientGraph.getVertices()) {
			for (Vertex vv : v.getVertices(Direction.BOTH, "similar")) {
			}
		}
	}

	@Override
	@SuppressWarnings("unused")
	public void findNodesOfAllEdges() {
		
		try {
			PrintWriter writer = new PrintWriter("orient");
			
			for(Edge e : orientGraph.getEdges()) {
				Vertex srcVertex = e.getVertex(Direction.OUT);
				Vertex dstVertex = e.getVertex(Direction.IN);
				
				writer.println(srcVertex.getProperty("nodeId") + "\t" + dstVertex.getProperty("nodeId"));
			}
			
			writer.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void findShortestPaths() {
		
		 OrientVertex v1 = (OrientVertex) orientGraph.getVertices("nodeId", 1).iterator().next();
	      for (int i : FindShortestPathBenchmark.generatedNodes) {
	        final OrientVertex v2 = (OrientVertex) orientGraph.getVertices("nodeId", i).iterator().next();

	        List<OIdentifiable> result = (List<OIdentifiable>) new OSQLFunctionShortestPath()
	        .execute(orientGraph, null, null, new Object[] { 
	        		v1.getRecord(), v2.getRecord(), Direction.OUT, 5 }, 
	        		new OBasicCommandContext());

	        int length = result.size();
	      }		
	}
}
