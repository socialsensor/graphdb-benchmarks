package eu.socialsensor.graphdatabases;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.VertexIDType;
import com.tinkerpop.gremlin.java.GremlinPipeline;

import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.TitanMassiveInsertion;
import eu.socialsensor.insert.TitanSingleInsertion;
import eu.socialsensor.query.Query;
import eu.socialsensor.query.TitanQuery;
import eu.socialsensor.utils.Utils;

/**
 * Titan graph database implementation
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 */

public class TitanGraphDatabase implements GraphDatabase{
	
	public static final String INSERTION_TIMES_OUTPUT_PATH = "data/titan.insertion.times";
	public static final String STORAGE_BACKEND = "local";
	
	double totalWeight;
	
	public TitanGraph titanGraph;
	public BatchGraph<TitanGraph> batchGraph;
	
	
	public static void main(String args[]) {
	}
		
	@Override
	public void open(String dbPath) {
		BaseConfiguration config = new BaseConfiguration();
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, STORAGE_BACKEND);
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, dbPath);
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL_KEY, false);
		titanGraph = TitanFactory.open(config);
	}
	
	@Override
	public void createGraphForSingleLoad(String dbPath) {
		BaseConfiguration config = new BaseConfiguration();
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, STORAGE_BACKEND);
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, dbPath);
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL_KEY, false);
		titanGraph = TitanFactory.open(config);
		titanGraph.makeKey("nodeId").dataType(String.class).indexed(Vertex.class).make();
		titanGraph.makeLabel("similar").unidirected().make();
		titanGraph.commit();
	}
	
	@Override
	public void createGraphForMassiveLoad(String dbPath) {
		BaseConfiguration config = new BaseConfiguration();
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "local");
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, dbPath);
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_BATCH_KEY, true);
        titanGraph = TitanFactory.open(config);
		titanGraph.makeKey("nodeId").dataType(String.class).indexed(Vertex.class).make();
		titanGraph.makeLabel("similar").make();
		titanGraph.commit();
		batchGraph = new BatchGraph<TitanGraph>(titanGraph, VertexIDType.STRING, 10000);
		batchGraph.setVertexIdKey("nodeId");
		batchGraph.setLoadingFromScratch(true);	
	}
	
	@Override
	public void massiveModeLoading(String dataPath) {
		Insertion titanMassiveInsertion = new TitanMassiveInsertion(this.batchGraph);
		titanMassiveInsertion.createGraph(dataPath);
	}
	
	@Override
	public void singleModeLoading(String dataPath) {
		Insertion titanSingleInsertion = new TitanSingleInsertion(this.titanGraph);
		titanSingleInsertion.createGraph(dataPath);
	}
	
	@Override
	public void shutdown() {
		if(titanGraph != null) {
			titanGraph.shutdown();
			titanGraph = null;
		}
	}
	
	@Override
	public void delete(String dbPath) {
		titanGraph = TitanFactory.open(dbPath);
		titanGraph.shutdown();
		TitanCleanup.clear(titanGraph);
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
		if(titanGraph != null) {
			batchGraph.shutdown();
			titanGraph.shutdown();
			batchGraph = null;
			titanGraph = null;
		}
	}
	
	@Override
	public void shorestPathQuery() {
		Query titanQuery = new TitanQuery(this.titanGraph);
		titanQuery.findShortestPaths();
	}

	@Override
	public void neighborsOfAllNodesQuery() {
		Query titanQuery = new TitanQuery(this.titanGraph);
		titanQuery.findNeighborsOfAllNodes();
	}

	@Override
	public void nodesOfAllEdgesQuery() {
		Query titanQuery = new TitanQuery(this.titanGraph);
		titanQuery.findNodesOfAllEdges();
	}
	
	@Override
	public int getNodeCount() {
		long nodeCount = new GremlinPipeline<Object, Object>(titanGraph).V().count();
		return (int)nodeCount;
	}
	
	
	@Override
	public Set<Integer> getNeighborsIds(int nodeId) {
		Set<Integer> neighbours = new HashSet<Integer>();
		Vertex vertex = titanGraph.getVertices("nodeId", String.valueOf(nodeId)).iterator().next();
		GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(vertex).out("similar");
		Iterator<Vertex> iter = pipe.iterator();
		while(iter.hasNext()) {
			String neighborId = iter.next().getProperty("nodeId");
			neighbours.add(Integer.valueOf(neighborId));
		}
		return neighbours;
	}
	
	
	@Override
	public double getNodeWeight(int nodeId) {
		Vertex vertex = titanGraph.getVertices("nodeId", String.valueOf(nodeId)).iterator().next();
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
		for(Vertex v: titanGraph.getVertices()) {
			v.setProperty("nodeCommunity", communityCounter);
			v.setProperty("community", communityCounter);
			communityCounter++;
		}
		
	}

	@Override
	public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
		Set<Integer> communities = new HashSet<>();
		Iterable<Vertex> vertices = titanGraph.getVertices("nodeCommunity", nodeCommunities);
		for(Vertex vertex : vertices) {
			GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(vertex).out("similar");
			Iterator<Vertex> iter = pipe.iterator();
			while(iter.hasNext()) {
				int community = iter.next().getProperty("community");
				communities.add(community);
			}
		}
		return communities;
	}

	@Override
	public Set<Integer> getNodesFromCommunity(int community) {
		Set<Integer> nodes = new HashSet<Integer>();
		Iterable<Vertex> iter = titanGraph.getVertices("community", community);
		for(Vertex v : iter) {
			String nodeIdString = v.getProperty("nodeId");
			nodes.add(Integer.valueOf(nodeIdString));
		}
		return nodes;
	}
	
	@Override
	public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
		Set<Integer> nodes = new HashSet<Integer>();
		Iterable<Vertex> iter = titanGraph.getVertices("nodeCommunity", nodeCommunity);
		for(Vertex v : iter) {
			String nodeIdString = v.getProperty("nodeId");
			nodes.add(Integer.valueOf(nodeIdString));
		}
		return nodes;
	}

	@Override
	public double getEdgesInsideCommunity(int vertexCommunity, int communityVertices) {
		double edges = 0;
		Iterable<Vertex> vertices = titanGraph.getVertices("nodeCommunity", vertexCommunity);
		Iterable<Vertex> comVertices = titanGraph.getVertices("community", communityVertices);
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
		Iterable<Vertex> iter = titanGraph.getVertices("community", community);
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
		Iterable<Vertex> iter = titanGraph.getVertices("nodeCommunity", nodeCommunity);
			for(Vertex vertex : iter) {
				nodeCommunityWeight += getNodeOutDegree(vertex);
			}
		return nodeCommunityWeight;
	}

	@Override
	public void moveNode(int nodeCommunity, int toCommunity) {
		Iterable<Vertex> fromIter = titanGraph.getVertices("nodeCommunity", nodeCommunity);
		for(Vertex vertex : fromIter) {
			vertex.setProperty("community", toCommunity);
		}
		
	}

	@Override
	public double getGraphWeightSum() {
		Iterable<Edge> edges = titanGraph.getEdges();
		return (double)Iterables.size(edges);
	}
		
	@Override
	public int reInitializeCommunities() {
		Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
		int communityCounter = 0;
		for(Vertex v : titanGraph.getVertices()) {
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
		Vertex vertex = titanGraph.getVertices("nodeCommunity", nodeCommunity).iterator().next();
		int community = vertex.getProperty("community");
		return community;
	}
	
	@Override
	public int getCommunityFromNode(int nodeId) {
		Vertex vertex = titanGraph.getVertices("nodeId", nodeId).iterator().next();
		return vertex.getProperty("community");
	}
	
	@Override
	public int getCommunitySize(int community) {
		Iterable<Vertex> vertices = titanGraph.getVertices("community", community);
		Set<Integer> nodeCommunities = new HashSet<Integer>();
		for(Vertex v : vertices) {
			int nodeCommunity = v.getProperty("nodeCommunity");
			nodeCommunities.add(nodeCommunity);
		}
		return nodeCommunities.size();
	}

	@Override
	public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
		Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();
		for(int i = 0; i < numberOfCommunities; i++) {
			Iterator<Vertex> verticesIter = titanGraph.getVertices("community", i).iterator();
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
