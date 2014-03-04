package eu.socialsensor.main;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;

import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.OrientMassiveInsertion;
import eu.socialsensor.insert.OrientSingleInsertion;

public class OrientGraphDatabase implements GraphDatabase{

	private OrientGraph orientGraph = null;
	private OrientGraphNoTx orientGraphNoTx = null;
	private Index<OrientVertex> vetrices = null;
	
	private Logger logger = Logger.getLogger(OrientGraphDatabase.class);
	
	public static void main(String args[]) {
		OrientGraphDatabase test = new OrientGraphDatabase();
		test.open("data/orientDB");
//		System.out.println(test.getNodeCount());
//		System.out.println(test.getNodeIds().size());
//		System.out.println(test.getNeighborsIds(1));
//		System.out.println(test.getNodeDegree(1));
	}
	
	@Override
	public void open(String dbPAth) {
		System.out.println("Opening OrientDB Graph Database . . . .");
//		logger.info("Opening OrientDB Graph Database . . . .");
		orientGraph = new OrientGraph("plocal:"+dbPAth);
		vetrices = orientGraph.getIndex("nodeId", OrientVertex.class);
		
		int counter = 0;
		for(Vertex v : orientGraph.getVertices()) {
			System.out.println(v);
			counter++;
		}
		System.out.println(counter);
	}
	
	@Override
	public void createGraphForSingleLoad(String dbPath) {
		System.out.println("Creating OrientDB Graph Database for single load . . . .");
		OGlobalConfiguration.DISK_CACHE_SIZE.setValue(5120); //this value depends of the installed memory
		orientGraph = new OrientGraph("plocal:"+dbPath);
		orientGraph.createIndex("nodeId", OrientVertex.class);
	    vetrices = orientGraph.getIndex("nodeId", OrientVertex.class);
	}
	
	@Override
	public void createGraphForMassiveLoad(String dbPath) {
		System.out.println("Creating OrientDB Graph Database for massive load . . . .");
//		logger.info("Creating OrientDB Graph Database for massive load . . . .");
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
		System.out.println("The OrientDB database is now shuting down . . . .");
		if(orientGraph != null) {
			orientGraph.drop();
			orientGraph.shutdown();
			orientGraph = null;
			vetrices = null;
		}
	}
	
	@Override
	public void shutdownMassiveGraph() {
		System.out.println("Shutting down OrientDB Graph Database for massive load");
//		logger.info("Shutting down OrientDB Graph Database for massive load");
		if(orientGraphNoTx != null) {
			orientGraphNoTx.shutdown();
			orientGraphNoTx = null;
			vetrices = null;
		}
	}

	@Override
	public int getNodeCount() {
		return (int)orientGraph.countVertices();
	}

	@Override
	public List<Long> getNodeIds() {
		List<Long> nodeIds = new ArrayList<Long>();
		for(Vertex v : orientGraph.getVertices()) {
			String nodeId = v.getProperty("nodeId");
			nodeIds.add(Long.valueOf(nodeId));
		}
		return nodeIds;
	}

	@Override
	public List<Long> getNeighborsIds(long nodeId) {
		List<Long> neighbours = new ArrayList<Long>();
		Vertex vertex = orientGraph.getVertices("nodeId", String.valueOf(nodeId)).iterator().next();
		long pipeInCount = new GremlinPipeline<String, Vertex>(vertex).in("similar").count();
		long pipeOutCount = new GremlinPipeline<String, Vertex>(vertex).out("similar").count();
		GremlinPipeline<String, Vertex> pipe;
		if(pipeInCount > pipeOutCount) {
			pipe = new GremlinPipeline<String, Vertex>(vertex).in("similar");
		}
		else {
			pipe = new GremlinPipeline<String, Vertex>(vertex).out("similar");
		}
		
		Iterator<Vertex> iter = pipe.iterator();
		while(iter.hasNext()) {
			String neighborId = iter.next().getProperty("nodeId");
			neighbours.add(Long.valueOf(neighborId));
		}
		return neighbours;
	}

	@Override
	public double getNodeDegree(long nodeId) {
		Vertex vertex = orientGraph.getVertices("nodeId", String.valueOf(nodeId)).iterator().next();
		GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(vertex).both("similar");
		return (double)pipe.count();
	}

	

	

	

	

	@Override
	public Iterable getNodes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void initCommunityProperty() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LinkedList<Vertex> getNodesFromCommunity(int community) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double getNodeInDegree(Vertex vertex) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getNodeOutDegree(Vertex vertex) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getCommunityWeight(int community) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getEdgesInsideCommunity(int nodes, int communityNodes) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void moveNode(int from, int to) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getNumberOfCommunities() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getGraphWeightSum() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getNodeCommunityWeight(int nodeCommunity) {
		// TODO Auto-generated method stub
		return 0;
	}

	

	@Override
	public void testCommunities() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reInitializeCommunities(Set<Integer> communityIds) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void printCommunities() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getCommunity(int nodeCommunity) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getCommunitySize(int community) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Set<Integer> getCommunityIds() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
		// TODO Auto-generated method stub
		return null;
	}
	
	private OrientGraphNoTx getGraphForMassiveLoad() {
		return this.orientGraphNoTx;
	}

	

}
