//package eu.socialsensor.main;
//
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Set;
//
//import com.tinkerpop.blueprints.Index;
//import com.tinkerpop.blueprints.Vertex;
//import com.tinkerpop.blueprints.impls.orient.OrientGraph;
//import com.tinkerpop.blueprints.impls.orient.OrientVertex;
//import com.tinkerpop.gremlin.java.GremlinPipeline;
//
//public class OrientGraphDatabase implements GraphDatabase{
//
//	private OrientGraph orientGraph = null;
//	Index<OrientVertex> vetrices = null;
//	
//	public static void main(String args[]) {
//		OrientGraphDatabase test = new OrientGraphDatabase();
//		test.open("data/orientDB");
////		System.out.println(test.getNodeCount());
////		System.out.println(test.getNodeIds().size());
////		System.out.println(test.getNeighborsIds(1));
////		System.out.println(test.getNodeDegree(1));
//	}
//	
//	@Override
//	public void open(String dbPAth) {
//		orientGraph = new OrientGraph("plocal:"+dbPAth);
//		vetrices = orientGraph.getIndex("nodeId", OrientVertex.class);
//	}
//
//	@Override
//	public int getNodeCount() {
//		return (int)orientGraph.countVertices();
//	}
//
//	@Override
//	public List<Long> getNodeIds() {
//		List<Long> nodeIds = new ArrayList<Long>();
//		for(Vertex v : orientGraph.getVertices()) {
//			String nodeId = v.getProperty("nodeId");
//			nodeIds.add(Long.valueOf(nodeId));
//		}
//		return nodeIds;
//	}
//
//	@Override
//	public List<Long> getNeighborsIds(long nodeId) {
//		List<Long> neighbours = new ArrayList<Long>();
//		Vertex vertex = orientGraph.getVertices("nodeId", String.valueOf(nodeId)).iterator().next();
//		long pipeInCount = new GremlinPipeline<String, Vertex>(vertex).in("similar").count();
//		long pipeOutCount = new GremlinPipeline<String, Vertex>(vertex).out("similar").count();
//		GremlinPipeline<String, Vertex> pipe;
//		if(pipeInCount > pipeOutCount) {
//			pipe = new GremlinPipeline<String, Vertex>(vertex).in("similar");
//		}
//		else {
//			pipe = new GremlinPipeline<String, Vertex>(vertex).out("similar");
//		}
//		
//		Iterator<Vertex> iter = pipe.iterator();
//		while(iter.hasNext()) {
//			String neighborId = iter.next().getProperty("nodeId");
//			neighbours.add(Long.valueOf(neighborId));
//		}
//		return neighbours;
//	}
//
//	@Override
//	public double getNodeDegree(long nodeId) {
//		Vertex vertex = orientGraph.getVertices("nodeId", String.valueOf(nodeId)).iterator().next();
//		GremlinPipeline<String, Vertex> pipe = new GremlinPipeline<String, Vertex>(vertex).both("similar");
//		return (double)pipe.count();
//	}
//
//	@Override
//	public void shutdown() {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void shutdownMassiveGraph() {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void createGraphForSingleLoad(String dbPath) {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void createGraphForMassiveLoad(String dbPath) {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public Iterable getNodes() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public void initCommunityProperty() {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public LinkedList<Vertex> getNodesFromCommunity(int community) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public double getNodeInDegree(Vertex vertex) {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//	@Override
//	public double getNodeOutDegree(Vertex vertex) {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//	@Override
//	public double getCommunityWeight(int community) {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//	@Override
//	public double getEdgesInsideCommunity(int nodes, int communityNodes) {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//	@Override
//	public void moveNode(int from, int to) {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public int getNumberOfCommunities() {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//	@Override
//	public double getGraphWeightSum() {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//	@Override
//	public double getNodeCommunityWeight(int nodeCommunity) {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//}
