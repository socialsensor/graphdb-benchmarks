package eu.socialsensor.graphdatabases;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;

import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.OrientMassiveInsertion;
import eu.socialsensor.insert.OrientSingleInsertion;
import eu.socialsensor.query.OrientQuery;
import eu.socialsensor.query.Query;
import eu.socialsensor.utils.Utils;

/**
 * OrientDB graph database implementation
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 */
public class OrientGraphDatabase implements GraphDatabase{

	private OrientGraph orientGraph = null;
	private OrientGraphNoTx orientGraphNoTx = null;
	private Index<OrientVertex> vetrices = null;
	
	public static void main(String args[]) {
	}	
	
	@Override
	public void open(String dbPAth) {
		orientGraph = new OrientGraph("plocal:"+dbPAth);
		vetrices = orientGraph.getIndex("nodeId", OrientVertex.class);
		
	}
	
	@Override
	public void createGraphForSingleLoad(String dbPath) {
		orientGraph = new OrientGraph("plocal:"+dbPath);
		orientGraph.createIndex("nodeId", OrientVertex.class);
	    vetrices = orientGraph.getIndex("nodeId", OrientVertex.class);
	}
	
	@Override
	public void createGraphForMassiveLoad(String dbPath) {
		OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
	    OGlobalConfiguration.TX_USE_LOG.setValue(false);
	    OGlobalConfiguration.ENVIRONMENT_CONCURRENT.setValue(false);
	    orientGraphNoTx = new OrientGraphNoTx("plocal:"+dbPath);
	    orientGraphNoTx.createIndex("nodeId", OrientVertex.class);
	    vetrices = orientGraphNoTx.getIndex("nodeId", OrientVertex.class);
	}
	
	@Override
	public void massiveModeLoading(String dataPath) {
		Insertion orientMassiveInsertion = new OrientMassiveInsertion(this.orientGraphNoTx, this.vetrices);
		orientMassiveInsertion.createGraph(dataPath);
	}
	
	@Override
	public void singleModeLoading(String dataPath) {
		Insertion orientSingleInsertion = new OrientSingleInsertion(this.orientGraph, this.vetrices);
		orientSingleInsertion.createGraph(dataPath);
	}
	
	@Override
	public void shutdown() {
		if(orientGraph != null) {
			orientGraph.shutdown();
			orientGraph = null;
			vetrices = null;
		}
	}
	
	@Override
	public void delete(String dbPath) {
		orientGraph = new OrientGraph("plocal:"+dbPath);
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
		if(orientGraphNoTx != null) {
			orientGraphNoTx.shutdown();
			orientGraphNoTx = null;
			vetrices = null;
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
		return (int)orientGraph.countVertices();
	}

	@Override
	public Set<Integer> getNeighborsIds(int nodeId) {
		Set<Integer> neighbours = new HashSet<Integer>();
		Vertex vertex = orientGraph.getVertices("nodeId", String.valueOf(nodeId)).iterator().next();
		GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(vertex).in("similar");
		Iterator<Vertex> iter = pipe.iterator();
		while(iter.hasNext()) {
			String neighborId = iter.next().getProperty("nodeId");
			neighbours.add(Integer.valueOf(neighborId));
		}
		return neighbours;
	}

	@Override
	public double getNodeWeight(int nodeId) {
		Vertex vertex = orientGraph.getVertices("nodeId", String.valueOf(nodeId)).iterator().next();
		double weight = getNodeOutDegree(vertex);
		return weight;
	}
	
	public double getNodeInDegree(Vertex vertex) {
		GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(vertex).in("similar");
		return (double)pipe.count();
	}

	public double getNodeOutDegree(Vertex vertex) {
		GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(vertex).out("similar");
		return (double)pipe.count();
	}

	@Override
	public void initCommunityProperty() {
		int communityCounter = 0;
		for(Vertex v: orientGraph.getVertices()) {
			v.setProperty("nodeCommunity", communityCounter);
			v.setProperty("community", communityCounter);
			communityCounter++;
		}
	}

	@Override
	public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
		Set<Integer> communities = new HashSet<>();
		Iterable<Vertex> vertices = orientGraph.getVertices("nodeCommunity", nodeCommunities);
		for(Vertex vertex : vertices) {
			GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(vertex).out("similar");
			Iterator<Vertex> iter = pipe.iterator();
			while(iter.hasNext()) {
				int community = iter.next().getProperty("community");
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
		for(Vertex v : iter) {
			String nodeIdString = v.getProperty("nodeId");
			nodes.add(Integer.valueOf(nodeIdString));
		}
		return nodes;
	}
	
	@Override
	public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
		Set<Integer> nodes = new HashSet<Integer>();
		Iterable<Vertex> iter = orientGraph.getVertices("nodeCommunity", nodeCommunity);
		for(Vertex v : iter) {
			String nodeIdString = v.getProperty("nodeId");
			nodes.add(Integer.valueOf(nodeIdString));
		}
		return nodes;
	}
	
	@Override
	public double getEdgesInsideCommunity(int vertexCommunity, int communityVertices) {
		double edges = 0;
		Iterable<Vertex> vertices = orientGraph.getVertices("nodeCommunity", vertexCommunity);
		Iterable<Vertex> comVertices = orientGraph.getVertices("community", orientGraph);
		for(Vertex vertex : vertices) {
			GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(vertex).out("similar");
			Iterator<Vertex> iter = pipe.iterator();
			while(iter.hasNext()) {
				if(Iterables.contains(comVertices, iter.next())){
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
		if(Iterables.size(iter) > 1) {
			for(Vertex vertex : iter) {
				communityWeight += getNodeOutDegree(vertex);
			}
		}
		return communityWeight;
	}
	
	@Override
	public double getNodeCommunityWeight(int nodeCommunity) {
		double nodeCommunityWeight = 0;
		Iterable<Vertex> iter = orientGraph.getVertices("nodeCommunity", nodeCommunity);
			for(Vertex vertex : iter) {
				nodeCommunityWeight += getNodeOutDegree(vertex);
			}
		return nodeCommunityWeight;
	}
	
	@Override
	public void moveNode(int nodeCommunity, int toCommunity) {
		Iterable<Vertex> fromIter = orientGraph.getVertices("nodeCommunity", nodeCommunity);
		for(Vertex vertex : fromIter) {
			vertex.setProperty("community", toCommunity);
		}
	}
		
	@Override
	public double getGraphWeightSum() {
		long edges = 0;
		for(Vertex o : orientGraph.getVertices()) {
			edges += ((OrientVertex)o).countEdges(Direction.OUT, "similar");
		}
		return (double)edges;
	}
		
	@Override
	public int reInitializeCommunities() {
		Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
		int communityCounter = 0;
		for(Vertex v : orientGraph.getVertices()) {
			int communityId = v.getProperty("community");
			if(!initCommunities.containsKey(communityId)) {
				initCommunities.put(communityId, communityCounter);
				communityCounter++;
			}
			int newCommunityId = initCommunities.get(communityId);
			v.setProperty("community", newCommunityId);
			v.setProperty("nodeCommunity", newCommunityId);
		}
		return communityCounter;
	}
	
	@Override
	public int getCommunity(int nodeCommunity) {
		Vertex vertex = orientGraph.getVertices("nodeCommunity", nodeCommunity).iterator().next();
		int community = vertex.getProperty("community");
		return community;
	}
	
	@Override
	public int getCommunityFromNode(int nodeId) {
		Vertex vertex = orientGraph.getVertices("nodeId", String.valueOf(nodeId)).iterator().next();
		return vertex.getProperty("community");
	}
	
	@Override
	public int getCommunitySize(int community) {
		Iterable<Vertex> vertices = orientGraph.getVertices("community", community);
		Set<Integer> nodeCommunities = new HashSet<Integer>();
		for(Vertex v : vertices) {
			int nodeCommunity = v.getProperty("nodeCommunity");
			if(!nodeCommunities.contains(nodeCommunity)) {
				nodeCommunities.add(nodeCommunity);
			}
		}
		return nodeCommunities.size();
	}
		
	@Override
	public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
		Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();
		for(int i = 0; i < numberOfCommunities; i++) {
			Iterator<Vertex> verticesIter = orientGraph.getVertices("community", i).iterator();
			List<Integer> vertices = new ArrayList<Integer>();
			while(verticesIter.hasNext()) {
				String nodeIdString = verticesIter.next().getProperty("nodeId");
				vertices.add(Integer.valueOf(nodeIdString));
			}
			communities.put(i, vertices);
		}
		return communities;
	}

	
}
