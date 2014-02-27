package eu.socialsensor.main;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.VertexIDType;
import com.tinkerpop.gremlin.java.GremlinPipeline;

public class TitanGraphDatabase implements GraphDatabase{
	
	public static final String INSERTION_TIMES_OUTPUT_PATH = "data/titan.insertion.times";
	public static final String STORAGE_BACKEND = "local";
	
	double totalWeight;
	
	public TitanGraph titanGraph;
	public BatchGraph<TitanGraph> batchGraph; 
//	private Map<Integer, Long> nodes = new HashMap<Integer, Long>();
	private Logger logger = Logger.getLogger(TitanGraphDatabase.class);
	
	public static void main(String args[]) {
		TitanGraphDatabase test = new TitanGraphDatabase();
		test.open("data/titan");
//		System.out.println(test.getNeighborsIds(1));
		//test.initCommunityProperty();
		test.test();
		
		test.shutdown();
	}
	
	public void testCommunities() {
		System.out.println("======================================");
		for(Vertex v : titanGraph.getVertices()) {
			System.out.println("Node "+v.getProperty("nodeId")+" ==> Community "+v.getProperty("community"));
		}
		System.out.println("======================================");
	}
	
	public void test() {
		int counter = 0;
//		for(Vertex v: titanGraph.getVertices()) {
//			if(counter < 4) {
//				v.setProperty("community", 1);
//			}
//			else if(counter < 6){
//				v.setProperty("community", 2);
//			}
//			else {
//				v.setProperty("community", 3);
//			}
//			counter++;
//		}
		for(Vertex v : titanGraph.getVertices()) {
			System.out.println(v.getProperty("community"));
		}
		System.out.println("==============");
		Iterable<Vertex> iter = titanGraph.getVertices("community", 1);
		for(Vertex v : iter) {
			System.out.println(v.getProperty("nodeId"));
		}
		
	}
	
	@Override
	public void open(String dbPath) {
		logger.info("Opening Titan Graph Database . . . .");
		BaseConfiguration config = new BaseConfiguration();
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, STORAGE_BACKEND);
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, dbPath);
        //storage.setProperty(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL_KEY, false);
		titanGraph = TitanFactory.open(config);		
	}
	
	@Override
	public void createGraphForSingleLoad(String dbPath) {
		logger.info("Creating Titan Graph Database for single load . . . .");
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
		logger.info("Creating Titan Graph Database for massive load . . . .");
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
	public void shutdown() {
		logger.info("The Titan database is now shuting down . . . .");
		if(titanGraph != null) {
			titanGraph.shutdown();
			titanGraph = null;
		}
	}


	@Override
	public void shutdownMassiveGraph() {
		logger.info("Massive Graph is shutting down . . . .");
		if(titanGraph != null) {
			batchGraph.shutdown();
			titanGraph.shutdown();
			batchGraph = null;
			titanGraph = null;
		}
	}
	
	@Override
	public Iterable getNodes() {
		return titanGraph.getVertices();
	}
	
	public List<Long> getNodeIds() {
		List<Long> nodeIds = new ArrayList<Long>();
		for(Vertex v : titanGraph.getVertices()) {
			String nodeId = v.getProperty("nodeId");
			nodeIds.add(Long.valueOf(nodeId));
		}
		return nodeIds;
	}
	
	public int getNodeCount() {
		int nodeCount = 0;
		for(Vertex v : titanGraph.getVertices()) {
			nodeCount++;
		}
		return nodeCount;
	}
	
	
	@Override
	public List<Long> getNeighborsIds(long nodeId) {
		List<Long> neighbours = new ArrayList<Long>();
		Vertex vertex = titanGraph.getVertices("nodeId", String.valueOf(nodeId)).iterator().next();
		long pipeInCount = new GremlinPipeline<String, Vertex>(vertex).in("similar").count();
		long pipeOutCount = new GremlinPipeline<String, Vertex>(vertex).out("similar").count();
		GremlinPipeline<String, Vertex> pipe;
		if(pipeInCount > pipeOutCount) {
			pipe = new GremlinPipeline<String, Vertex>(vertex).in("similar");
		}
		else {
			pipe = new GremlinPipeline<String, Vertex>(vertex).out("similar");
		}
		
		Iterator<Vertex> iter 			= pipe.iterator();
		while(iter.hasNext()) {
			String neighborId = iter.next().getProperty("nodeId");
			neighbours.add(Long.valueOf(neighborId));
		}
		return neighbours;
	}
	
	@Override
	public double getNodeDegree(long nodeId) {
		Vertex vertex = titanGraph.getVertices("nodeId", String.valueOf(nodeId)).iterator().next();
		GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(vertex).both("similar");
		return (double)pipe.count();
	}
	
	@Override
	public double getNodeInDegree(Vertex vertex) {
		GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(vertex).in("similar");
		return (double)pipe.count();
	}

	@Override
	public double getNodeOutDegree(Vertex vertex) {
		GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(vertex).out("similar");
		return (double)pipe.count();
	}

	@Override
	public void initCommunityProperty() {
		int communityCounter = 0;
		for(Vertex v: titanGraph.getVertices()) {
			v.setProperty("community", communityCounter++);
		}
		
	}

	@Override
	public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
		Set<Integer> communities = new HashSet<>();
		Iterable<Vertex> vertices = titanGraph.getVertices("community", nodeCommunities);
		for(Vertex vertex : vertices) {
			GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(vertex).in("similar");
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
	public LinkedList<Vertex> getNodesFromCommunity(int community) {
		LinkedList<Vertex> nodes = new LinkedList<Vertex>();
		Iterable<Vertex> iter = titanGraph.getVertices("community", community);
		for(Vertex v : iter) {
			nodes.add(v);
		}
		return nodes;
	}

	@Override
	public double getEdgesInsideCommunity(int vertexCommunity, int communityVertices) {
		double edges = 0;
		Iterable<Vertex> vertices = titanGraph.getVertices("community", vertexCommunity);
		Iterable<Vertex> comVertices = titanGraph.getVertices("community", communityVertices);
		for(Vertex vertex : vertices) {
			GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(vertex).in("similar");
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
				communityWeight += getNodeInDegree(vertex);
			}
		}
		return communityWeight;
	}
	
	@Override
	public double getNodeCommunityWeight(int nodeCommunity) {
		double nodeCommunityWeight = 0;
		Iterable<Vertex> iter = titanGraph.getVertices("community", nodeCommunity);
			for(Vertex vertex : iter) {
				nodeCommunityWeight += getNodeInDegree(vertex);
			}
		return nodeCommunityWeight;
	}

	@Override
	public void moveNode(int from, int to) {
		Iterable<Vertex> fromIter = titanGraph.getVertices("community", from);
		for(Vertex vertex : fromIter) {
			vertex.setProperty("community", to);
		}
		
	}

	@Override
	public int getNumberOfCommunities() {
		Set<Integer> communities = new HashSet<Integer>();
		for(Vertex v : titanGraph.getVertices()) {
			int community = v.getProperty("community");
			if(!communities.contains(community)) {
				communities.add(community);
			}
		}
		return communities.size();
	}

	@Override
	public double getGraphWeightSum() {
		int count = 0;;
		for(Edge e : titanGraph.getEdges()) {
			count++;
		}
		return (double)count;
	}

		

}
