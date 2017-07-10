/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package eu.socialsensor.graphdatabases;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.driver.GremlinManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.structure.GraphElement;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Path;
import com.baidu.hugegraph.structure.graph.Vertex;

import com.baidu.hugegraph.structure.gremlin.Result;
import com.baidu.hugegraph.structure.gremlin.ResultSet;
import com.baidu.hugegraph.structure.schema.VertexLabel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Direction;
import eu.socialsensor.insert.HugeGraphMassiveInsertion;
import eu.socialsensor.insert.HugeGraphSingleInsertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.HugeGraphUtils;

/**
 * Created by zhangsuochao on 17/6/21.
 */
public class HugeGraphDatabase extends
        GraphDatabaseBase<Iterator<Vertex>, Iterator<Edge>, Vertex, Edge> {

    protected HugeClient hugeClient = null;
    protected GremlinManager gremlin = null;
    protected final BenchmarkConfiguration conf;
    public static final String NODE = "node";
    public static final int NODEIDINDEX = 5;

    public HugeGraphDatabase(BenchmarkConfiguration config,
                             File dbStorageDirectoryIn) {
        super(GraphDatabaseType.HUGEGRAPH_CASSANDRA, dbStorageDirectoryIn);
        conf = config;
    }

    @Override
    public Vertex getOtherVertexFromEdge(Edge edge, Vertex oneVertex) {
        if (edge.source().equals(oneVertex.id())) {
            return hugeClient.graph().getVertex(edge.target());
        } else {
            return hugeClient.graph().getVertex(edge.source());
        }
    }

    @Override
    public Vertex getSrcVertexFromEdge(Edge edge) {
        return hugeClient.graph().getVertex(edge.source());
    }

    @Override
    public Vertex getDestVertexFromEdge(Edge edge) {
        return hugeClient.graph().getVertex(edge.target());
    }

    @Override
    public Vertex getVertex(Integer i) {
        return hugeClient.graph().getVertex(HugeGraphUtils.createId(NODE,
                i.toString()));
    }

    @Override
    public Iterator<Edge> getAllEdges() {
        return hugeClient.graph().getEdges(1000).iterator();
    }

    @Override
    public Iterator<Edge> getNeighborsOfVertex(Vertex v) {
        ResultSet resultSet = gremlin.gremlin(
                String.format("g.V('%s').bothE()", v.id())).execute();
        Iterator<Result> it = resultSet.iterator();
        List<Edge> edges = new LinkedList<>();
        while (it.hasNext()) {
            edges.add(it.next().getEdge());
        }
        return edges.iterator();
    }

    @Override
    public boolean edgeIteratorHasNext(Iterator<Edge> it) {
        return it.hasNext();
    }

    @Override
    public Edge nextEdge(Iterator<Edge> it) {
        return it.next();
    }

    @Override
    public void cleanupEdgeIterator(Iterator<Edge> it) {

    }

    @Override
    public Iterator<Vertex> getVertexIterator() {
        return hugeClient.graph().getVertices(1000).iterator();
    }

    @Override
    public boolean vertexIteratorHasNext(Iterator<Vertex> it) {
        return it.hasNext();
    }

    @Override
    public Vertex nextVertex(Iterator<Vertex> it) {
        return it.next();
    }

    @Override
    public void cleanupVertexIterator(Iterator<Vertex> it) {

    }

    @Override
    public void open() {
        buildGraphEnv();
    }

    @Override
    public void createGraphForSingleLoad() {
        buildGraphEnv();
    }

    @Override
    public void createGraphForMassiveLoad() {
        buildGraphEnv();
    }

    @Override
    public void massiveModeLoading(File dataPath) {
        HugeGraphMassiveInsertion massiveInsertion =
                new HugeGraphMassiveInsertion(this.hugeClient.graph());
        massiveInsertion.createGraph(dataPath, 0);
    }

    @Override
    public void singleModeLoading(File dataPath,
                                  File resultsPath, int scenarioNumber) {
        HugeGraphSingleInsertion singleInsertion = new HugeGraphSingleInsertion(
                this.hugeClient.graph(), resultsPath);
        singleInsertion.createGraph(dataPath, scenarioNumber);
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void delete() {

    }

    @Override
    public void shutdownMassiveGraph() {

    }

    static int counter = 0;
    @Override
    public void shortestPath(Vertex fromNode, Integer node) {
        System.out.println(">>>>>" + counter++ + " round,(from node: "
                + fromNode.id() + ", to node: " + node + ")");
        String targetId  = HugeGraphUtils.createId(NODE, node.toString());
        String query = String.format("g.V('%s').repeat(out().simplePath())"
                + ".until(hasId('%s').or().loops().is(gt(2)))" + ".hasId('%s')"
                + ".path().limit(1)", fromNode.id(), targetId, targetId);
        ResultSet resultSet = gremlin.gremlin(query).execute();

        Iterator<Result> results = resultSet.iterator();
        results.forEachRemaining(result -> {
            //System.out.println(result.getObject().getClass());
            Object object = result.getObject();
            if (object instanceof Vertex) {
                System.out.println(((Vertex) object).id());
            } else if (object instanceof Edge) {
                System.out.println(((Edge) object).id());
            } else if (object instanceof Path) {
                List<GraphElement> elements = ((Path) object).objects();
                elements.stream().forEach(element -> {
                    //System.out.println(element.getClass());
                    System.out.println(element);
                });
            } else {
                System.out.println(object);
            }
        });
    }

    @Override
    public int getNodeCount() {
        String query = String.format("g.V().count()");
        ResultSet resultSet = gremlin.gremlin(query).execute();
        return (int) resultSet.iterator().next().getObject();
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId) {
        Set<Integer> neighbors = new HashSet<Integer>();
        Vertex vertex = getVerticesByProperty(
                NODE_ID, nodeId).iterator().next();
        Iterable<Vertex> vertices = getVertices(vertex, Direction.OUT, SIMILAR);
        Iterator<Vertex> iter = vertices.iterator();
        while (iter.hasNext()) {
            Integer neighborId =
                    Integer.valueOf(iter.next().id().substring(NODEIDINDEX));
            neighbors.add(neighborId);
        }
        return neighbors;
    }

    @Override
    public double getNodeWeight(int nodeId) {
        Vertex vertex = getVerticesByProperty(NODE_ID, nodeId)
                .iterator().next();
        double weight = getNodeOutDegree(vertex);
        return weight;
    }

    @Override
    public void initCommunityProperty() {
        int communityCounter = 0;
        for (Vertex v : hugeClient.graph().getVertices()) {
            v.property(NODE_COMMUNITY, communityCounter);
            v.property(COMMUNITY, communityCounter);
            Vertex vertex = vertexWithoutId(v);
            hugeClient.graph().addVertices(ImmutableList.of(vertex));
            communityCounter++;
        }
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(
            int nodeCommunities) {
        Set<Integer> communities = new HashSet<Integer>();
        Iterable<Vertex> vertices = getVerticesByProperty(NODE_COMMUNITY,
                nodeCommunities);
        for (Vertex vertex : vertices) {
            Iterable<Vertex> neighVertices = getVertices(vertex, Direction.OUT,
                    SIMILAR);
            Iterator<Vertex> iter = neighVertices.iterator();
            while (iter.hasNext()) {
                int community = (int) iter.next().properties().get(COMMUNITY);
                communities.add(community);
            }
        }
        return communities;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community) {
        Set<Integer> nodes = new HashSet<Integer>();
        Iterable<Vertex> iter = getVerticesByProperty(COMMUNITY, community);
        for (Vertex v : iter) {
            Integer nodeId = Integer.valueOf(v.id().substring(NODEIDINDEX));
            nodes.add(nodeId);
        }
        return nodes;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
        Set<Integer> nodes = new HashSet<Integer>();
        Iterable<Vertex> iter = getVerticesByProperty(NODE_COMMUNITY,
                nodeCommunity);
        for (Vertex v : iter) {
            Integer nodeId = Integer.valueOf(v.id().substring(NODEIDINDEX));
            nodes.add(nodeId);
        }
        return nodes;
    }

    @Override
    public double getEdgesInsideCommunity(int nodeCommunity,
                                          int communityNodes) {
        double edges = 0;
        Iterable<Vertex> vertices = getVerticesByProperty(NODE_COMMUNITY,
                nodeCommunity);
        Iterable<Vertex> comVertices = getVerticesByProperty(COMMUNITY,
                communityNodes);
        for (Vertex vertex : vertices) {
            for (Vertex v : getVertices(vertex, Direction.OUT, SIMILAR)) {
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
        Iterable<Vertex> iter = getVerticesByProperty(COMMUNITY, community);
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
        Iterable<Vertex> iter = getVerticesByProperty(NODE_COMMUNITY,
                nodeCommunity);
        for (Vertex vertex : iter) {
            nodeCommunityWeight += getNodeOutDegree(vertex);
        }
        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int from, int to) {
        Iterable<Vertex> fromIter = getVerticesByProperty(NODE_COMMUNITY, from);
        for (Vertex vertex : fromIter) {
            vertex.property(COMMUNITY, to);
            Vertex v = vertexWithoutId(vertex);
            hugeClient.graph().addVertices(ImmutableList.of(v));
        }
    }

    @Override
    public double getGraphWeightSum() {
        String query = String.format("g.E().count()");
        ResultSet resultSet = gremlin.gremlin(query).execute();
        return ((Integer) resultSet.iterator().next().getObject())
                .doubleValue();
    }

    @Override
    public int reInitializeCommunities() {

        Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
        int communityCounter = 0;
        for (Vertex v : hugeClient.graph().getVertices()) {
            int communityId = (int) v.properties().get(COMMUNITY);
            if (!initCommunities.containsKey(communityId)) {
                initCommunities.put(communityId, communityCounter);
                communityCounter++;
            }
            int newCommunityId = initCommunities.get(communityId);
            v.property(COMMUNITY, newCommunityId);
            v.property(NODE_COMMUNITY, newCommunityId);
            Vertex vertex = vertexWithoutId(v);
            hugeClient.graph().addVertices(ImmutableList.of(vertex));
        }
        return communityCounter;
    }

    @Override
    public int getCommunityFromNode(int nodeId) {
        Vertex vertex = getVerticesByProperty(NODE_ID, nodeId)
                .iterator().next();
        return (int) vertex.properties().get(COMMUNITY);
    }

    @Override
    public int getCommunity(int nodeCommunity) {
        Vertex vertex = getVerticesByProperty(NODE_COMMUNITY, nodeCommunity)
                .iterator().next();
        return (int) vertex.properties().get(COMMUNITY);

    }

    @Override
    public int getCommunitySize(int community) {
        Iterable<Vertex> vertices = getVerticesByProperty(COMMUNITY, community);
        Set<Integer> nodeCommunities = new HashSet<Integer>();
        for (Vertex v : vertices) {
            int nodeCommunity = (int) v.properties().get(NODE_COMMUNITY);
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
            Iterable<Vertex> vertexlist = getVerticesByProperty(COMMUNITY, i);
            Iterator<Vertex> verticesIter = vertexlist.iterator();
            List<Integer> vertices = new ArrayList<Integer>();
            while (verticesIter.hasNext()) {
                String id = verticesIter.next().id().substring(NODEIDINDEX);
                Integer nodeId = Integer.valueOf(id);
                vertices.add(nodeId);
            }
            communities.put(i, vertices);
        }
        return communities;
    }

    @Override
    public boolean nodeExists(int nodeId) {
        return false;
    }

    private void buildGraphEnv() {
        hugeClient = HugeClient.open(conf.getHugegraphUrl(),
                conf.getHugegraphGraph());
        gremlin = hugeClient.gremlin();
        SchemaManager schemaManager = hugeClient.schema();
        schemaManager.makePropertyKey(NODE_ID).asText().ifNotExist().create();
        schemaManager.makePropertyKey(COMMUNITY).asInt().ifNotExist().create();
        schemaManager.makePropertyKey(NODE_COMMUNITY).asInt().ifNotExist()
                .create();

        VertexLabel node = schemaManager.makeVertexLabel(NODE)
                .properties(NODE_ID, COMMUNITY, NODE_COMMUNITY)
                .primaryKeys(NODE_ID).ifNotExist().create();
        schemaManager.makeEdgeLabel(SIMILAR).link(NODE, NODE)
                .ifNotExist().create();
        /**
         *schemaManager.makeIndexLabel("nodeByCommunity")
         *       .on(node).secondary().by(COMMUNITY).create();
         *schemaManager.makeIndexLabel("nodeByNodeCommunity")
         *       .on(node).secondary().by(NODE_COMMUNITY).create();
         */
    }

    private Iterable<Vertex> getVerticesByProperty(String pKey, int pValue) {
        String query = String.format("g.V().hasLabel('node')has('%s', %s)",
                pKey, pValue);
        ResultSet resultSet = gremlin.gremlin(query).execute();
        Iterator<Result> it = resultSet.iterator();
        List<Vertex> vertices = new LinkedList<>();
        while (it.hasNext()) {
            vertices.add(it.next().getVertex());
        }
        return vertices;
    }

    public double getNodeOutDegree(Vertex vertex) {
        return getNodeDegree(vertex, Direction.OUT);
    }

    public double getNodeInDegree(Vertex vertex) {
        return getNodeDegree(vertex, Direction.IN);
    }

    public double getNodeDegree(Vertex vertex, Direction direction) {
        String direct = direction.equals(Direction.OUT) ? "out" : "in";
        String query = String.format("g.V('%s').%s(%s).count()",
                vertex.id(), direct, SIMILAR);
        ResultSet resultSet = gremlin.gremlin(query).execute();
        Iterator<Result> it = resultSet.iterator();
        return (double) it.next().getObject();
    }

    private Iterable<Vertex> getVertices(Vertex vertex, Direction direct,
                                         String edgetype) {
        String vertexId = vertex == null ? "" : vertex.id();
        String direction = direct.equals(Direction.OUT) ? "out" : "in";
        String query = String.format("g.V('%s').%s(%s)%s",
                vertexId, direction, SIMILAR);
        ResultSet resultSet = gremlin.gremlin(query).execute();
        Iterator<Result> it = resultSet.iterator();
        List<Vertex> vertices = new LinkedList<>();
        while (it.hasNext()) {
            vertices.add(it.next().getVertex());
        }
        return vertices;
    }

    private Vertex vertexWithoutId(Vertex v) {
        Vertex vertex = new Vertex(NODE).property(NODE_ID, v.id());
        Map<String, Object> props = v.properties();
        Iterator it = props.keySet().iterator();
        String key;
        while (it.hasNext()) {
            key = (String) it.next();
            if (!key.equals(NODE_ID)) {
                vertex.property(key, props.get(key));
            }
        }
        return vertex;
    }
}
