package eu.socialsensor.query;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe.LoopBundle;

import eu.socialsensor.benchmarks.FindShortestPathBenchmark;
import eu.socialsensor.main.GraphDatabaseBenchmark;

/**
 * Query implementation for Titan graph database
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 */

public class TitanQuery implements Query {
	
	private TitanGraph titanGraph = null;
	
	public static void main(String args[]) {
//		GraphDatabase graph = new TitanGraphDatabase();
//		graph.createGraphForMassiveLoad(GraphDatabaseBenchmark.TITANDB_PATH);
//		graph.massiveModeLoading("./data/youtubeEdges.txt");
//		graph.shutdownMassiveGraph();
		
		TitanQuery titanQuery = new TitanQuery();
		titanQuery.findNodesOfAllEdges();
	}
	
	public TitanQuery(TitanGraph titanGraph) {
		this.titanGraph = titanGraph;
	}
	
	public TitanQuery() {
		this.titanGraph = TitanFactory.build()
				.set("storage.backend", "berkeleyje")
				.set("storage.transactions", false)
				.set("storage.directory", GraphDatabaseBenchmark.TITANDB_PATH)
				.open();
	}
	
	@Override
	@SuppressWarnings("unused")
	public void findNeighborsOfAllNodes() {
		for (Vertex v : titanGraph.getVertices()) {
			for (Vertex vv : v.getVertices(Direction.BOTH, "similar")) {
			}
		}
	}
	
	@Override
	@SuppressWarnings("unused")
	public void findNodesOfAllEdges() {
		
		try {
			PrintWriter writer = new PrintWriter("orient");
			
			for(Edge e : titanGraph.getEdges()) {
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
		Vertex v1 = titanGraph.getVertices("nodeId", 1).iterator().next();
		
		for(int i : FindShortestPathBenchmark.generatedNodes) {
			final Vertex v2 = titanGraph.getVertices("nodeId", i).iterator().next();
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
