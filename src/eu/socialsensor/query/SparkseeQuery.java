package eu.socialsensor.query;

import com.sparsity.sparksee.algorithms.SinglePairShortestPathBFS;
import com.sparsity.sparksee.gdb.EdgeData;
import com.sparsity.sparksee.gdb.EdgesDirection;
import com.sparsity.sparksee.gdb.Graph;
import com.sparsity.sparksee.gdb.Objects;
import com.sparsity.sparksee.gdb.ObjectsIterator;
import com.sparsity.sparksee.gdb.Session;
import com.sparsity.sparksee.gdb.Value;

import eu.socialsensor.benchmarks.FindShortestPathBenchmark;

/**
 * Query implementation for Sparksee graph database
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 */

public class SparkseeQuery implements Query {
	
	private Session session;
	private Graph sparkseeGraph;
	
	public SparkseeQuery(Session session) {
		this.session = session;
		this.sparkseeGraph = session.getGraph();
	}
	
	@Override
	public void findNeighborsOfAllNodes() {
		int nodeType = sparkseeGraph.findType("node");
		int edgeType = sparkseeGraph.findType("similar");
		Objects objects = sparkseeGraph.select(nodeType);
		ObjectsIterator iter = objects.iterator();
		while(iter.hasNext()) {
			long nodeID = iter.next();
			Objects neighbors = sparkseeGraph.neighbors(nodeID, edgeType, EdgesDirection.Any);
			neighbors.close();
		}
		iter.close();
		objects.close();
	}

	@Override
	public void findNodesOfAllEdges() {
		int edgeType = sparkseeGraph.findType("similar");
		Objects objects = sparkseeGraph.select(edgeType);
		ObjectsIterator iter = objects.iterator();
		while(iter.hasNext()) {
			long edgeID = iter.next();
			EdgeData edge = sparkseeGraph.getEdgeData(edgeID);
			@SuppressWarnings("unused")
			long srcNodeID = edge.getHead();
			@SuppressWarnings("unused")
			long dstNodeID = edge.getTail();
		}
		iter.close();
		objects.close();
	}

	@Override
	public void findShortestPaths() {
		@SuppressWarnings("unused")
		double length = 0;
		int nodeType = sparkseeGraph.findType("node");
		int nodeAttribute = sparkseeGraph.findAttribute(nodeType, "nodeId");
		int edgeType = sparkseeGraph.findType("similar");
		Value value = new Value();
		long srcNodeID = sparkseeGraph.findObject(nodeAttribute, value.setString("1"));
		for(int i : FindShortestPathBenchmark.generatedNodes) {
			long dstNodeID = sparkseeGraph.findObject(nodeAttribute, value.setString(String.valueOf(i)));
			SinglePairShortestPathBFS shortestPathBFS = new SinglePairShortestPathBFS(session, srcNodeID, dstNodeID);
			shortestPathBFS.addNodeType(nodeType);
			shortestPathBFS.addEdgeType(edgeType, EdgesDirection.Outgoing);
			shortestPathBFS.setMaximumHops(4);
			shortestPathBFS.run();
			if(shortestPathBFS.exists()) {
				length = shortestPathBFS.getCost();
			}
			shortestPathBFS.close();
		}
	}

}
