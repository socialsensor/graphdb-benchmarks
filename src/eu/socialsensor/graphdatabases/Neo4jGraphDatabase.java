package eu.socialsensor.graphdatabases;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.google.common.collect.Iterables;

import eu.socialsensor.clustering.LouvainMethodCache;
import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.Neo4jMassiveInsertion;
import eu.socialsensor.insert.Neo4jSingleInsertion;
import eu.socialsensor.query.Neo4jQuery;
import eu.socialsensor.query.Query;
import eu.socialsensor.utils.Utils;

@SuppressWarnings("deprecation")
public class Neo4jGraphDatabase implements GraphDatabase {

	GraphDatabaseService neo4jGraph = null;
	private Index<Node> nodeIndex = null;
	
	private BatchInserter inserter = null;
	private BatchInserterIndexProvider indexProvider = null;
	private BatchInserterIndex nodes = null;
	
	public static enum RelTypes implements RelationshipType {
	    SIMILAR
	}
	
	public static Label NODE_LABEL = DynamicLabel.label("node");
	
	public static void main(String args[]) {
	}
	
	@Override
	public void open(String dbPath) {
		System.out.println("Opening Neo4j Graph Database . . . .");
		neo4jGraph = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		try(Transaction tx = neo4jGraph.beginTx()) {
			nodeIndex = neo4jGraph.index().forNodes("nodes");
			tx.success();
			tx.close();
		}
		
	}

	@Override
	public void createGraphForSingleLoad(String dbPath) {
		System.out.println("Creating Neo4j Graph Database for single load . . . .");
		neo4jGraph = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		try ( Transaction tx = neo4jGraph.beginTx() ) {
			nodeIndex = neo4jGraph.index().forNodes("nodes");
			tx.success();
			tx.close();
		}
	}

	@Override
	public void createGraphForMassiveLoad(String dbPath) {
		System.out.println("Creating Neo4j Graph Database for massive load . . . .");
		Map<String, String> config = new HashMap<String, String>();
		config.put("cache_type", "none");
		config.put("use_memory_mapped_buffers", "true");
		config.put("neostore.nodestore.db.mapped_memory", "200M");
		config.put("neostore.relationshipstore.db.mapped_memory", "1000M");
		config.put("neostore.propertystore.db.mapped_memory", "250M");
		config.put("neostore.propertystore.db.strings.mapped_memory", "250M");
		inserter = BatchInserters.inserter(dbPath, config);
		indexProvider = new LuceneBatchInserterIndexProvider(inserter);
		nodes = indexProvider.nodeIndex("nodes", MapUtil.stringMap("type", "exact"));
	}
	
	@Override
	public void singleModeLoading(String dataPath) {
		Insertion neo4jSingleInsertion = new Neo4jSingleInsertion(this.neo4jGraph, this.nodeIndex);
		neo4jSingleInsertion.createGraph(dataPath);
	}
	
	@Override
	public void massiveModeLoading(String dataPath) {
		Insertion neo4jMassiveInsertion = new Neo4jMassiveInsertion(this.inserter, this.nodes);
		neo4jMassiveInsertion.createGraph(dataPath);
	}
	
	@Override
	public void shutdown() {
		System.out.println("The Neo4j database is now shuting down . . . .");
		if(neo4jGraph != null) {
			neo4jGraph.shutdown();
			nodeIndex = null;
		}
	}
	
	@Override
	public void delete(String dbPath) {
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
		System.out.println("Shutting down Neo4j graph for massive load . . . .");
		if(inserter != null) {
			indexProvider.shutdown();
			inserter.shutdown();
			indexProvider = null;
			inserter = null;
		}
	}
	
	@Override
	public void shorestPathQuery() {
		Query neo4jQuery = new Neo4jQuery(this.neo4jGraph);
		neo4jQuery.findShortestPaths();
	}

	@Override
	public void neighborsOfAllNodesQuery() {
		Query neo4jQuery = new Neo4jQuery(this.neo4jGraph);
		neo4jQuery.findNeighborsOfAllNodes();
	}

	@Override
	public void nodesOfAllEdgesQuery() {
		Query neo4jQuery = new Neo4jQuery(this.neo4jGraph);
		neo4jQuery.findNodesOfAllEdges();
	}
	
	@Override
	public int getNodeCount() {
		int nodeCount;
		try (Transaction tx = neo4jGraph.beginTx()) {
			nodeCount = IteratorUtil.count(GlobalGraphOperations.at(neo4jGraph).getAllNodes());
			tx.success();
			tx.close();
		}
		return nodeCount;
	}

	@Override
	public Set<Integer> getNeighborsIds(int nodeId) {
		Set<Integer> neighbors = new HashSet<Integer>();
		try (Transaction tx = neo4jGraph.beginTx()) {
			Node n = nodeIndex.get("nodeId", nodeId).getSingle();
			for(Relationship relationship : n.getRelationships(RelTypes.SIMILAR, Direction.OUTGOING)) {
				Node neighbour = relationship.getOtherNode(n);
				String neighbourId = (String)neighbour.getProperty("nodeId");
				neighbors.add(Integer.valueOf(neighbourId));
			}
			tx.success();
//			tx.close();
		}
		return neighbors;
	}
	
	@Override
	public double getNodeWeight(int nodeId) {
		double weight;
		try(Transaction tx = neo4jGraph.beginTx()) {
			Node n = nodeIndex.get("nodeId", nodeId).getSingle();
			weight =  getNodeOutDegree(n);
			tx.success();
			tx.close();
		}
		return weight;
	}
	
	public double getNodeInDegree(Node node) {
		Iterable<Relationship> rel = node.getRelationships(Direction.OUTGOING, RelTypes.SIMILAR);
		return (double)(IteratorUtil.count(rel));
	}


	public double getNodeOutDegree(Node node) {
		Iterable<Relationship> rel = node.getRelationships(Direction.INCOMING, RelTypes.SIMILAR);
		return (double)(IteratorUtil.count(rel));
	}

	@Override
	public void initCommunityProperty() {
		int communityCounter = 0;
		int transactionCounter = 0;
		Transaction tx = ((GraphDatabaseAPI)neo4jGraph).tx().unforced().begin();
		for(Node n : GlobalGraphOperations.at(neo4jGraph).getAllNodes()) {
			n.setProperty("nodeCommunity", communityCounter);
			n.setProperty("community", communityCounter);
			communityCounter++;
			transactionCounter++;
			if(transactionCounter == LouvainMethodCache.CACHE_SIZE) {
				transactionCounter = 0;
				tx.success();
				tx.close();
				tx = ((GraphDatabaseAPI)neo4jGraph).tx().unforced().begin();
			}
		}
		tx.success();
		tx.close();
		
	}

	@Override
	public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
		Set<Integer> communities = new HashSet<Integer>();
		try(Transaction tx = neo4jGraph.beginTx()) {
			ResourceIterable<Node> nodes = neo4jGraph.findNodesByLabelAndProperty(Neo4jGraphDatabase.NODE_LABEL, "nodeCommunity", nodeCommunities);
			for(Node n : nodes) {
				for(Relationship r : n.getRelationships(RelTypes.SIMILAR, Direction.OUTGOING)) {
					Node neighbour = r.getOtherNode(n);
					int community = (int)(neighbour.getProperty("community"));
					communities.add(community);
				}
			}
			tx.success();
			tx.close();
		}
		return communities;
	}
	
	@Override
	public Set<Integer> getNodesFromCommunity(int community) {
		Set<Integer> nodes = new HashSet<Integer>();
		try(Transaction tx = neo4jGraph.beginTx()) {
			ResourceIterable<Node> iter = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, "community", community);
			for(Node n : iter) {
				String nodeIdString = (String)(n.getProperty("nodeId"));
				nodes.add(Integer.valueOf(nodeIdString));
			}
			tx.success();
			tx.close();
		}
		return nodes;
	}
	
	@Override
	public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
		Set<Integer> nodes = new HashSet<Integer>();
		try(Transaction tx = neo4jGraph.beginTx()) {
			ResourceIterable<Node> iter = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, "nodeCommunity", nodeCommunity);
			for(Node n : iter) {
				String nodeIdString = (String)(n.getProperty("nodeId"));
				nodes.add(Integer.valueOf(nodeIdString));
			}
			tx.success();
			tx.close();
		}
		return nodes;
	}

	@Override
	public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes) {
		double edges = 0;
		try(Transaction tx = neo4jGraph.beginTx()) {
			ResourceIterable<Node> nodes = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, "nodeCommunity", nodeCommunity);
			ResourceIterable<Node> comNodes = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, "community", communityNodes);
			for(Node node : nodes) {
				Iterable<Relationship> relationships = node.getRelationships(RelTypes.SIMILAR, Direction.OUTGOING);
				for(Relationship r : relationships) {
					Node neighbor = r.getOtherNode(node);
					if(Iterables.contains(comNodes, neighbor)) {
						edges++;
					}
				}
			}
			tx.success();
			tx.close();
		}
		
		return edges;
	}

	@Override
	public double getCommunityWeight(int community) {
		double communityWeight = 0;
		try(Transaction tx = neo4jGraph.beginTx()) {
			ResourceIterable<Node> iter = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, "community", community);
			if(Iterables.size(iter) > 1) {
				for(Node n : iter) {
					communityWeight += getNodeOutDegree(n);
				}
			}
			tx.success();
			tx.close();
		}
		return communityWeight;
	}
	
	@Override
	public double getNodeCommunityWeight(int nodeCommunity) {
		double nodeCommunityWeight = 0;
		try(Transaction tx = neo4jGraph.beginTx()) {
			ResourceIterable<Node> iter = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, "nodeCommunity", nodeCommunity);
			if(Iterables.size(iter) > 1) {
				for(Node n : iter) {
					nodeCommunityWeight += getNodeOutDegree(n);
				}
			}
			tx.success();
			tx.close();
		}
		return nodeCommunityWeight;
	}

	@Override
	public void moveNode(int nodeCommunity, int toCommunity) {
		try(Transaction tx = ((GraphDatabaseAPI)neo4jGraph).tx().unforced().begin()) {
			ResourceIterable<Node> fromIter = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, "nodeCommunity", nodeCommunity);
			for(Node node : fromIter) {
				node.setProperty("community", toCommunity);
			}
			tx.success();
			tx.close();
		}		
	}

	@Override
	public double getGraphWeightSum() {
		int edgeCount;
		try (Transaction tx = neo4jGraph.beginTx()) {
			edgeCount = IteratorUtil.count(GlobalGraphOperations.at(neo4jGraph).getAllRelationships());
			tx.success();
			tx.close();
		}
		return (double)edgeCount;
	}
	
	@Override
	public int reInitializeCommunities() {
		Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
		int communityCounter = 0;
		try(Transaction tx = ((GraphDatabaseAPI)neo4jGraph).tx().unforced().begin()) {
			for(Node n : GlobalGraphOperations.at(neo4jGraph).getAllNodes()) {
				int communityId = (int)(n.getProperty("community"));
				if(!initCommunities.containsKey(communityId)) {
					initCommunities.put(communityId, communityCounter);
					communityCounter++;
				}
				int newCommunityId = initCommunities.get(communityId);
				n.setProperty("community", newCommunityId);
				n.setProperty("nodeCommunity", newCommunityId);
			}
			tx.success();
			tx.close();
		}
		
		return communityCounter;
	}

	@Override
	public int getCommunity(int nodeCommunity) {
		int community = 0;
		try(Transaction tx = neo4jGraph.beginTx()) {
			Node node = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, "nodeCommunity", nodeCommunity).iterator().next();
			community = (int)(node.getProperty("community"));
			tx.success();
			tx.close();
		}
		return community;
	}
	
	@Override
	public int getCommunityFromNode(int nodeId) {
		int community = 0;
		try(Transaction tx = neo4jGraph.beginTx()) {
			Node node = nodeIndex.get("nodeId", nodeId).getSingle();
			community = (int)(node.getProperty("community"));
			tx.success();
			tx.close();
		}
		return community;
	}

	@Override
	public int getCommunitySize(int community) {
		Set<Integer> nodeCommunities = new HashSet<Integer>();
		try(Transaction tx = neo4jGraph.beginTx()) {
			ResourceIterable<Node> nodes = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, "community", community);
			for(Node n : nodes) {
				int nodeCommunity = (int)(n.getProperty("community"));
				nodeCommunities.add(nodeCommunity);
			}
			tx.success();
			tx.close();
		}
		return nodeCommunities.size();
	}

	@Override
	public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
		Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();
		try(Transaction tx = neo4jGraph.beginTx()) {
			for(int i = 0; i < numberOfCommunities; i++) {
				ResourceIterable<Node> nodesIter = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, "community", i);
				List<Integer> nodes = new ArrayList<Integer>();
				for(Node n : nodesIter) {
					String nodeIdString = (String)(n.getProperty("nodeId"));
					nodes.add(Integer.valueOf(nodeIdString));
				}
				communities.put(i, nodes);
			}
			tx.success();
			tx.close();
		}
		return communities;
	}

	
}
