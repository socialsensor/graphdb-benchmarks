package eu.socialsensor.graphdatabases;

import com.google.common.collect.Iterables;
import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientExtendedGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import com.tinkerpop.blueprints.impls.orient.asynch.OrientGraphAsynch;
import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.OrientMassiveInsertion;
import eu.socialsensor.insert.OrientSingleInsertion;
import eu.socialsensor.main.GraphDatabaseBenchmark;
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
	
	private OrientExtendedGraph graph = null;
	private String useLightWeightEdges = "true";

	public OrientGraphDatabase() {
		OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.setValue("nothing");
		useLightWeightEdges = GraphDatabaseBenchmark.inputPropertiesFile.getProperty("ORIENTDB_LW_EDGES");
	}

	public static void main(String args[]) {
		GraphDatabase db = new OrientGraphDatabase();
//		db.createGraphForMassiveLoad(GraphDatabaseBenchmark.ORIENTDB_PATH);
//		db.massiveModeLoading("datasets/real/amazonEdges.txt");
//    	db.shutdownMassiveGraph();
    
		db.open(GraphDatabaseBenchmark.ORIENTDB_PATH);
		db.shorestPathQuery();
		db.shutdown();
	}

	@Override
	public void open(String dbPath) {
		graph = getGraph(dbPath);
	}

	@Override
	public void createGraphForSingleLoad(String dbPath) {
		OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
		graph = getGraph(dbPath);
		try {
			createSchema();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void createGraphForMassiveLoad(String dbPath) {
		OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
		graph = getGraph(dbPath);
		try {
			createSchema();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}

  @Override
  public void massiveModeLoading(String dataPath) {
    OrientMassiveInsertion orientMassiveInsertion = new OrientMassiveInsertion(this.graph.getRawGraph().getURL());
    orientMassiveInsertion.createGraph(dataPath);
  }

	@Override
	public void singleModeLoading(String dataPath) {
		Insertion orientSingleInsertion = new OrientSingleInsertion(this.graph);
		orientSingleInsertion.createGraph(dataPath);
	}

	@Override
	public void shutdown() {
		if (graph != null) {
			graph.shutdown();
			graph = null;
		}
	}

	@Override
	public void delete(String dbPath) {
		OrientGraphNoTx g = new OrientGraphNoTx("plocal:" + dbPath);
		g.drop();

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
		if (graph != null) {
			graph.shutdown();
			graph = null;
		}
	}

	@Override
	public void shorestPathQuery() {
		Query orientQuery = new OrientQuery(this.graph);
		orientQuery.findShortestPaths();
	}

	@Override
	public void neighborsOfAllNodesQuery() {
		Query orientQuery = new OrientQuery(this.graph);
		orientQuery.findNeighborsOfAllNodes();
	}

  @Override
  public void nodesOfAllEdgesQuery() {
    Query orientQuery = new OrientQuery(this.graph);
    orientQuery.findNodesOfAllEdges();
  }

  @Override
  public int getNodeCount() {
    return (int) graph.countVertices();
  }

  @Override
  public Set<Integer> getNeighborsIds(int nodeId) {
    Set<Integer> neighbours = new HashSet<Integer>();
    Vertex vertex = graph.getVertices("nodeId", nodeId).iterator().next();
    for (Vertex v : vertex.getVertices(Direction.IN, "similar")) {
      Integer neighborId = v.getProperty("nodeId");
      neighbours.add(neighborId);
    }
    return neighbours;
  }

  @Override
  public double getNodeWeight(int nodeId) {
    Vertex vertex = graph.getVertices("nodeId", nodeId).iterator().next();
    double weight = getNodeOutDegree(vertex);
    return weight;
  }

  public double getNodeInDegree(Vertex vertex) {
    @SuppressWarnings("rawtypes")
	OMultiCollectionIterator result = (OMultiCollectionIterator) vertex.getVertices(Direction.IN, "similar");
    return (double) result.size();
  }

  public double getNodeOutDegree(Vertex vertex) {
    @SuppressWarnings("rawtypes")
	OMultiCollectionIterator result = (OMultiCollectionIterator) vertex.getVertices(Direction.OUT, "similar");
    return (double) result.size();
  }

  @Override
  public void initCommunityProperty() {
    int communityCounter = 0;
    for (Vertex v : graph.getVertices()) {
      ((OrientVertex) v).setProperties("nodeCommunity", communityCounter, "community", communityCounter);
      ((OrientVertex) v).save();
      communityCounter++;
    }
  }

  @Override
	public Set<Integer> getCommunitiesConnectedToNodeCommunities( int nodeCommunities) {
     Set<Integer> communities = new HashSet<Integer>();
     Iterable<Vertex> vertices = graph.getVertices("nodeCommunity", nodeCommunities);
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
    Iterable<Vertex> iter = graph.getVertices("community", community);
    for (Vertex v : iter) {
      Integer nodeId = v.getProperty("nodeId");
      nodes.add(nodeId);
    }
    return nodes;
  }

  @Override
  public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
    Set<Integer> nodes = new HashSet<Integer>();
    Iterable<Vertex> iter = graph.getVertices("nodeCommunity", nodeCommunity);
    for (Vertex v : iter) {
      Integer nodeId = v.getProperty("nodeId");
      nodes.add(nodeId);
    }
    return nodes;
  }

  @Override
  public double getEdgesInsideCommunity(int vertexCommunity, int communityVertices) {
    double edges = 0;
    Iterable<Vertex> vertices = graph.getVertices("nodeCommunity", vertexCommunity);
    Iterable<Vertex> comVertices = graph.getVertices("community", communityVertices);
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
    Iterable<Vertex> iter = graph.getVertices("community", community);
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
    Iterable<Vertex> iter = graph.getVertices("nodeCommunity", nodeCommunity);
    for (Vertex vertex : iter) {
      nodeCommunityWeight += getNodeOutDegree(vertex);
    }
    return nodeCommunityWeight;
  }

  @Override
  public void moveNode(int nodeCommunity, int toCommunity) {
    Iterable<Vertex> fromIter = graph.getVertices("nodeCommunity", nodeCommunity);
    for (Vertex vertex : fromIter) {
      vertex.setProperty("community", toCommunity);
    }
  }

  @Override
  public double getGraphWeightSum() {
    long edges = 0;
    for (Vertex o : graph.getVertices()) {
      edges += ((OrientVertex) o).countEdges(Direction.OUT, "similar");
    }
    return (double) edges;
  }

  @Override
  public int reInitializeCommunities() {
    Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
    int communityCounter = 0;
    for (Vertex v : graph.getVertices()) {
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
    final Iterator<Vertex> result = graph.getVertices("nodeCommunity", nodeCommunity).iterator();
    if (!result.hasNext())
      throw new IllegalArgumentException("node community not found: " + nodeCommunity);

    Vertex vertex = result.next();
    int community = vertex.getProperty("community");
    return community;
  }

  @Override
  public int getCommunityFromNode(int nodeId) {
    Vertex vertex = graph.getVertices("nodeId", nodeId).iterator().next();
    return vertex.getProperty("community");
  }

  @Override
  public int getCommunitySize(int community) {
    Iterable<Vertex> vertices = graph.getVertices("community", community);
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
      Iterator<Vertex> verticesIter = graph.getVertices("community", i).iterator();
      List<Integer> vertices = new ArrayList<Integer>();
      while (verticesIter.hasNext()) {
        Integer nodeId = verticesIter.next().getProperty("nodeId");
        vertices.add(nodeId);
      }
      communities.put(i, vertices);
    }
    return communities;
  }

  	protected void createSchema() {
  		OrientExtendedGraph g = graph;

  		if (g instanceof OrientGraphAsynch) {
  			((OrientGraphAsynch) g).execute(new OCallable<Object, OrientBaseGraph>() {
  				@Override
  				public Object call(OrientBaseGraph g) {
  					createSchemaInternal(g);
  					return null;
  				}
  			});
  		} 
  		else
  			createSchemaInternal(g);
  	}

  	private void createSchemaInternal(OrientExtendedGraph g) {
  		((OrientGraph) g).executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
  			@SuppressWarnings({ "unchecked", "rawtypes" })
  			@Override
  			public Object call(final OrientBaseGraph g) {
  				OrientVertexType v = g.getVertexBaseType();
  				v.createProperty("nodeId", OType.INTEGER);
  				v.createEdgeProperty(Direction.OUT, "similar", OType.LINKBAG);
  				v.createEdgeProperty(Direction.IN, "similar", OType.LINKBAG);

  				OrientEdgeType similar = g.createEdgeType("similar");
  				similar.createProperty("out", OType.LINK, v);
  				similar.createProperty("in", OType.LINK, v);

  				g.createKeyIndex("community", Vertex.class, new Parameter("type", "NOTUNIQUE_HASH_INDEX"), 
  						new Parameter("keytype", "INTEGER"));
  				g.createKeyIndex("nodeCommunity", Vertex.class, new Parameter("type", "NOTUNIQUE_HASH_INDEX"), 
  						new Parameter("keytype", "INTEGER"));

  				g.createKeyIndex("nodeId", Vertex.class, new Parameter("type", "UNIQUE_HASH_INDEX"), 
  						new Parameter("keytype", "INTEGER"));
  				return null;
  			}
  		});
  	}

  	private OrientExtendedGraph getGraph(final String dbPath) {
  		OrientExtendedGraph g;
  		OrientGraphFactory graphFactory = new OrientGraphFactory("plocal:" + dbPath);
  		g = graphFactory.getTx().setUseLog(false);
  		if("false".equals(useLightWeightEdges)) {
  			((OrientGraph)g).setUseLightweightEdges(false);
  		}
  		return g;
  	}

  	@Override
  	public boolean nodeExists(int nodeId) {
  		Iterable<Vertex> iter = graph.getVertices("nodeId", nodeId);
  		return iter.iterator().hasNext();
  	}
}
