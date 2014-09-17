package eu.socialsensor.query;

import java.util.Iterator;
import java.util.List;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
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
	public void findNeighborsOfAllNodes() {
		for(Vertex v : titanGraph.getVertices()) {
			GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(v).both("similar");
			Iterator<Vertex> neighbors = pipe.iterator();
			while(neighbors.hasNext()) {
			}
		}		
	}
	
	@Override
	public void findNodesOfAllEdges() {
		for(Edge e : titanGraph.getEdges()) {
			GremlinPipeline<String, Vertex> getNodesPipe = new GremlinPipeline<String, Vertex>(e).bothV();
			Iterator<Vertex> vertexIter = getNodesPipe.iterator();
			while(vertexIter.hasNext()) {
			}
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
			int length = pathPipe.iterator().next().size() - 1;
			
		}
	}
	
}
