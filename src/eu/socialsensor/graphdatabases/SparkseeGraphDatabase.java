package eu.socialsensor.graphdatabases;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sparsity.sparksee.gdb.AttributeKind;
import com.sparsity.sparksee.gdb.DataType;
import com.sparsity.sparksee.gdb.Database;
import com.sparsity.sparksee.gdb.Graph;
import com.sparsity.sparksee.gdb.Session;
import com.sparsity.sparksee.gdb.Sparksee;
import com.sparsity.sparksee.gdb.SparkseeConfig;

import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.SparkseeMassiveInsertion;
import eu.socialsensor.insert.SparkseeSingleInsertion;
import eu.socialsensor.utils.Utils;

public class SparkseeGraphDatabase implements GraphDatabase {
	
	public static final String INSERTION_TIMES_OUTPUT_PATH = "data/sparksee.insertion.times";
	
	double totalWeight;
	
	SparkseeConfig sparkseeConfig = new SparkseeConfig();
	Sparksee sparksee =  new Sparksee(sparkseeConfig);
	Database database;
	Session session;
	Graph sparkseeGraph;
	
	@Override
	public void open(String dbPath) {
		try {
			database = sparksee.open(dbPath + "/SparkseeDB.gdb", true);
			session = database.newSession();
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void createGraphForSingleLoad(String dbPath) {
		try {
			new File("data/SparkseeDB").mkdir();
			database = sparksee.create(dbPath + "/SparkseeDB.gdb", "SparkseeDB");
			session = database.newSession();
			sparkseeGraph = session.getGraph();
			int node = sparkseeGraph.newNodeType("node");
			sparkseeGraph.newAttribute(node, "nodeId", DataType.String, AttributeKind.Unique);
			sparkseeGraph.newEdgeType("similar", false, false);
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void createGraphForMassiveLoad(String dbPath) {
		//maybe some more configuration?
		try {
			new File("data/SparkseeDB").mkdir();
			database = sparksee.create(dbPath + "/SparkseeDB.gdb", "SparkseeDB");
			session = database.newSession();
			sparkseeGraph = session.getGraph();
			int node = sparkseeGraph.newNodeType("node");
			sparkseeGraph.newAttribute(node, "nodeId", DataType.String, AttributeKind.Unique);
			sparkseeGraph.newEdgeType("similar", false, false);
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void massiveModeLoading(String dataPath) {
		Insertion sparkseeMassiveInsertion = new SparkseeMassiveInsertion(session);
		sparkseeMassiveInsertion.createGraph(dataPath);
	}

	@Override
	public void singleModeLoading(String dataPath) {
		Insertion sparkseeSingleInsertion = new SparkseeSingleInsertion(this.session);
		sparkseeSingleInsertion.createGraph(dataPath);
	}

	@Override
	public void shutdown() {
		if(session != null) {
			session.close();
			session = null;
			database.close();
			database = null;
			sparksee.close();
			sparksee = null;
		}
		
	}
	
	@Override
	public void shutdownMassiveGraph() {
		shutdown();	
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
	public void shorestPathQuery() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void neighborsOfAllNodesQuery() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nodesOfAllEdgesQuery() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getNodeCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Set<Integer> getNeighborsIds(int nodeId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double getNodeWeight(int nodeId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initCommunityProperty() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Set<Integer> getCommunitiesConnectedToNodeCommunities(
			int nodeCommunities) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Integer> getNodesFromCommunity(int community) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getCommunityWeight(int community) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getNodeCommunityWeight(int nodeCommunity) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void moveNode(int from, int to) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public double getGraphWeightSum() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int reInitializeCommunities() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getCommunityFromNode(int nodeId) {
		// TODO Auto-generated method stub
		return 0;
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
	public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
		// TODO Auto-generated method stub
		return null;
	}

}
