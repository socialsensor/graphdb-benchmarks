package eu.socialsensor.graphdatabases;

import com.google.common.collect.Iterables;
import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.index.OIndex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.OrientMassiveInsertion;
import eu.socialsensor.insert.OrientSingleInsertion;
import eu.socialsensor.query.OrientQuery;
import eu.socialsensor.query.Query;
import eu.socialsensor.utils.Utils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * OrientDB graph database implementation
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 */
public class OrientGraphDatabase implements GraphDatabase {
	
	private OrientBaseGraph orientGraph = null;
	private OIndex vertices = null;

	private boolean clusteringWorkload = false;

	public OrientGraphDatabase() {
		OGlobalConfiguration.USE_WAL.setValue(false);
	}

	public static void main(String args[]) {
	}

	@Override
	public void open(String dbPath) {
		orientGraph = getGraph(dbPath);
		if(clusteringWorkload) {
//			orientGraph.setWarnOnForceClosingTx(true);
			orientGraph.createKeyIndex("community", Vertex.class, new Parameter("type", "NOTUNIQUE_HASH_INDEX"), 
					new Parameter("keytype", "INTEGER"));
			orientGraph.createKeyIndex("nodeCommunity", Vertex.class, new Parameter("type", "NOTUNIQUE_HASH_INDEX"), 
					new Parameter("keytype", "INTEGER"));
//			orientGraph.setWarnOnForceClosingTx(true);
		}
	}

	@Override
	public void createGraphForSingleLoad(String dbPath) {
		orientGraph = getGraph(dbPath);
//		orientGraph.setWarnOnForceClosingTx(true);
		orientGraph.createKeyIndex("nodeId", Vertex.class, new Parameter("type", "UNIQUE_HASH_INDEX"), 
				new Parameter("keytype", "INTEGER"));
//		graph.createKeyIndex("community", Vertex.class, new Parameter("type", "NOTUNIQUE_HASH_INDEX"), 
//				new Parameter("keytype", "INTEGER"));
//		graph.createKeyIndex("nodeCommunity", Vertex.class, new Parameter("type", "NOTUNIQUE_HASH_INDEX"), 
//				new Parameter("keytype", "INTEGER"));
//		vertices = orientGraph.getRawGraph().getMetadata().getIndexManager().getIndex("V.nodeId");
//		orientGraph.setWarnOnForceClosingTx(true);
	}

	@Override
	public void createGraphForMassiveLoad(String dbPath) {  
		OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(true);
		OGlobalConfiguration.TX_USE_LOG.setValue(false);
		OGlobalConfiguration.ENVIRONMENT_CONCURRENT.setValue(false);
		orientGraph = getGraph(dbPath);
//		graph.createKeyIndex("community", Vertex.class, new Parameter("type", "NOTUNIQUE_HASH_INDEX"), 
//				new Parameter("keytype", "INTEGER"));
//		graph.createKeyIndex("nodeCommunity", Vertex.class, new Parameter("type", "NOTUNIQUE_HASH_INDEX"), 
//				new Parameter("keytype", "INTEGER"));
		orientGraph.createKeyIndex("nodeId", Vertex.class, new Parameter("type", "UNIQUE_HASH_INDEX"), 
    			new Parameter("keytype", "INTEGER"));
		vertices = orientGraph.getRawGraph().getMetadata().getIndexManager().getIndex("V.nodeId");
	}

	@Override
	public void massiveModeLoading(String dataPath) {
		Insertion orientMassiveInsertion = new OrientMassiveInsertion(this.orientGraph, vertices);
		orientMassiveInsertion.createGraph(dataPath);
	}

	@Override
	public void singleModeLoading(String dataPath) {
		Insertion orientSingleInsertion = new OrientSingleInsertion(this.orientGraph, vertices);
		orientSingleInsertion.createGraph(dataPath);
	}

	@Override
	public void shutdown() {
		if (orientGraph != null) {
			orientGraph.shutdown();
			orientGraph = null;
		}
	}

	@Override
	public void delete(String dbPath) {
		orientGraph = new OrientGraphNoTx("plocal:" + dbPath);
		orientGraph.drop();
		try {
			Thread.sleep(6000);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		Utils utils = new Utils();
		utils.deleteRecursively(new File(dbPath));
	}

	@Override
	public void shutdownMassiveGraph() {
		if (orientGraph != null) {
			orientGraph.shutdown();
			orientGraph = null;
			vertices = null;
		}
	}

	@Override
	public void shorestPathQuery() {
		Query orientQuery = new OrientQuery(this.orientGraph);
		orientQuery.findShortestPaths();
	}

	@Override
	public void neighborsOfAllNodesQuery() {
		Query orientQuery = new OrientQuery(this.orientGraph);
		orientQuery.findNeighborsOfAllNodes();
	}

	@Override
	public void nodesOfAllEdgesQuery() {
		Query orientQuery = new OrientQuery(this.orientGraph);
		orientQuery.findNodesOfAllEdges();
	}

	@Override
	public int getNodeCount() {
		return (int) orientGraph.countVertices();
	}

	@Override
	public Set<Integer> getNeighborsIds(int nodeId) {
		Set<Integer> neighbours = new HashSet<Integer>();
		Vertex vertex = orientGraph.getVertices("nodeId", nodeId).iterator().next();
		for (Vertex v : vertex.getVertices(Direction.IN, "similar")) {
			Integer neighborId = v.getProperty("nodeId");
			neighbours.add(neighborId);
		}
		return neighbours;
	}

	@Override
	public double getNodeWeight(int nodeId) {
		Vertex vertex = orientGraph.getVertices("nodeId", nodeId).iterator().next();
		double weight = getNodeOutDegree(vertex);
		return weight;
	}

	public double getNodeInDegree(Vertex vertex) {
		OMultiCollectionIterator result = (OMultiCollectionIterator) vertex.getVertices(Direction.IN, "similar");
		return (double) result.size();
	}

	public double getNodeOutDegree(Vertex vertex) {
		OMultiCollectionIterator result = (OMultiCollectionIterator) vertex.getVertices(Direction.OUT, "similar");
		return (double) result.size();
	}

	@Override
	public void initCommunityProperty() {
		int communityCounter = 0;
		for (Vertex v : orientGraph.getVertices()) {
			((OrientVertex) v).setProperties("nodeCommunity", communityCounter, "community", communityCounter);
			((OrientVertex) v).save();
			communityCounter++;
		}
	}

	@Override
	public Set<Integer> getCommunitiesConnectedToNodeCommunities( int nodeCommunities) {
		Set<Integer> communities = new HashSet<>();
		Iterable<Vertex> vertices = orientGraph.getVertices("nodeCommunity", nodeCommunities);
		for(Vertex vertex : vertices) {
			for( Vertex v : vertex.getVertices(Direction.OUT, "similar")) {
				int community = v.getProperty("community");
				if(!communities.contains(community)) {
					communities.add(community);
				}
			}
		}
		return communities;
	}

	@Override
	public Set<Integer> getNodesFromCommunity(int community) {
		Set<Integer> nodes = new HashSet<Integer>();
		Iterable<Vertex> iter = orientGraph.getVertices("community", community);
		for (Vertex v : iter) {
			Integer nodeId = v.getProperty("nodeId");
			nodes.add(nodeId);
		}
		return nodes;
	}

	@Override
	public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
		Set<Integer> nodes = new HashSet<Integer>();
		Iterable<Vertex> iter = orientGraph.getVertices("nodeCommunity", nodeCommunity);
		for (Vertex v : iter) {
			Integer nodeId = v.getProperty("nodeId");
			nodes.add(nodeId);
		}
		return nodes;
	}

	@Override
	public double getEdgesInsideCommunity(int vertexCommunity, int communityVertices) {
		double edges = 0;
		Iterable<Vertex> vertices = orientGraph.getVertices("nodeCommunity", vertexCommunity);
		Iterable<Vertex> comVertices = orientGraph.getVertices("community", communityVertices);
		for (Vertex vertex : vertices) {
			for (Vertex v : vertex.getVertices(Direction.OUT, "similar")) {
				if (Iterables.contains(comVertices, v)) {
					edges++;
				}
			}
		}
		return edges;
	}

	@Override
	public double getCommunityWeight(int community) {
		double communityWeight = 0;
		Iterable<Vertex> iter = orientGraph.getVertices("community", community);
		if (Iterables.size(iter) > 1) {
			for (Vertex vertex : iter) {
				communityWeight += getNodeOutDegree(vertex);
			}
		}
		return communityWeight;
	}

	@Override
	public double getNodeCommunityWeight(int nodeCommunity) {
		double nodeCommunityWeight = 0;
		Iterable<Vertex> iter = orientGraph.getVertices("nodeCommunity", nodeCommunity);
		for (Vertex vertex : iter) {
			nodeCommunityWeight += getNodeOutDegree(vertex);
		}
		return nodeCommunityWeight;
	}

	@Override
	public void moveNode(int nodeCommunity, int toCommunity) {
		Iterable<Vertex> fromIter = orientGraph.getVertices("nodeCommunity", nodeCommunity);
		for (Vertex vertex : fromIter) {
			vertex.setProperty("community", toCommunity);
		}
	}

	@Override
	public double getGraphWeightSum() {
		long edges = 0;
		for (Vertex o : orientGraph.getVertices()) {
			edges += ((OrientVertex) o).countEdges(Direction.OUT, "similar");
		}
		return (double) edges;
	}

	@Override
	public int reInitializeCommunities() {
		Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
		int communityCounter = 0;
		for (Vertex v : orientGraph.getVertices()) {
			int communityId = v.getProperty("community");
			if (!initCommunities.containsKey(communityId)) {
				initCommunities.put(communityId, communityCounter);
				communityCounter++;
			}
			int newCommunityId = initCommunities.get(communityId);
			((OrientVertex) v).setProperties("community", newCommunityId, "nodeCommunity", newCommunityId);
			((OrientVertex) v).save();
		}
		return communityCounter;
	}

	@Override
	public int getCommunity(int nodeCommunity) {
		final Iterator<Vertex> result = orientGraph.getVertices("nodeCommunity", nodeCommunity).iterator();
		if (!result.hasNext()) {
			throw new IllegalArgumentException("node community not found: " + nodeCommunity);
		}
		Vertex vertex = result.next();
		int community = vertex.getProperty("community");
		return community;
	}

	@Override
	public int getCommunityFromNode(int nodeId) {
		Vertex vertex = orientGraph.getVertices("nodeId", nodeId).iterator().next();
		return vertex.getProperty("community");
	}

	@Override
	public int getCommunitySize(int community) {
		Iterable<Vertex> vertices = orientGraph.getVertices("community", community);
		Set<Integer> nodeCommunities = new HashSet<Integer>();
		for (Vertex v : vertices) {
			int nodeCommunity = v.getProperty("nodeCommunity");
			if (!nodeCommunities.contains(nodeCommunity)) {
				nodeCommunities.add(nodeCommunity);
			}
		}
		return nodeCommunities.size();
	}

	@Override
	public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
		Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();
		for (int i = 0; i < numberOfCommunities; i++) {
			Iterator<Vertex> verticesIter = orientGraph.getVertices("community", i).iterator();
			List<Integer> vertices = new ArrayList<Integer>();
			while (verticesIter.hasNext()) {
				Integer nodeId = verticesIter.next().getProperty("nodeId");
				vertices.add(nodeId);
			}
			communities.put(i, vertices);
		}
		return communities;
	}

	@Override
	public void setClusteringWorkload(boolean isClusteringWorkload) {
		this.clusteringWorkload = isClusteringWorkload;
	}

	private OrientBaseGraph getGraph(String dbPath) {
		OrientGraphFactory graphFactory = new OrientGraphFactory("plocal:" + dbPath);
		return graphFactory.getNoTx();
	}
}
