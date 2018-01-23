/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package eu.socialsensor.graphdatabases;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.example.ExampleUtil;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.tinkerpop.blueprints.Direction;

import eu.socialsensor.insert.HugeGraphCoreMassiveInsertion;
import eu.socialsensor.insert.HugeGraphCoreSingleInsertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;

public class HugeGraphCoreDatabase extends GraphDatabaseBase<
                                           Iterator<Vertex>,
                                           Iterator<Edge>,
                                           HugeVertex,
                                           HugeEdge> {

    private static final Logger LOG = LogManager.getLogger();

    private HugeGraph graph = null;

    private static final String NODE = "node";
    private static int counter = 0;

    public HugeGraphCoreDatabase(BenchmarkConfiguration config,
                                 File dbStorageDirectoryIn) {
        super(GraphDatabaseType.HUGEGRAPH_CORE, dbStorageDirectoryIn);
    }

    @Override
    public HugeVertex getOtherVertexFromEdge(HugeEdge edge,
                                             HugeVertex oneVertex) {
        return edge.otherVertex(oneVertex);
    }

    @Override
    public HugeVertex getSrcVertexFromEdge(HugeEdge edge) {
        return edge.sourceVertex();
    }

    @Override
    public HugeVertex getDestVertexFromEdge(HugeEdge edge) {
        return edge.targetVertex();
    }

    @Override
    public HugeVertex getVertex(Integer i) {
        return (HugeVertex) this.graph.vertices(i).next();
    }

    @Override
    public Iterator<Edge> getAllEdges() {
        return this.graph.edges();
    }

    @Override
    public Iterator<Edge> getNeighborsOfVertex(HugeVertex v) {
        return v.edges(org.apache.tinkerpop.gremlin.structure.Direction.BOTH,
                       SIMILAR);
    }

    @Override
    public boolean edgeIteratorHasNext(Iterator<Edge> it) {
        return it.hasNext();
    }

    @Override
    public HugeEdge nextEdge(Iterator<Edge> it) {
        return (HugeEdge) it.next();
    }

    @Override
    public void cleanupEdgeIterator(Iterator<Edge> it) {
    }

    @Override
    public Iterator<Vertex> getVertexIterator() {
        return this.graph.vertices();
    }

    @Override
    public boolean vertexIteratorHasNext(Iterator<Vertex> it) {
        return it.hasNext();
    }

    @Override
    public HugeVertex nextVertex(Iterator<Vertex> it) {
        return (HugeVertex) it.next();
    }

    @Override
    public void cleanupVertexIterator(Iterator<Vertex> it) {
    }

    /* Massive insertion */
    @Override
    public void createGraphForMassiveLoad() {
        buildGraphEnv(true);
    }

    @Override
    public void massiveModeLoading(File dataPath) {
        HugeGraphCoreMassiveInsertion insertion =
                new HugeGraphCoreMassiveInsertion(this.graph);
        insertion.createGraph(dataPath, 0);
    }

    @Override
    public void shutdownMassiveGraph() {
    }

    /* Single insertion */
    @Override
    public void createGraphForSingleLoad() {
        buildGraphEnv(true);
    }

    @Override
    public void singleModeLoading(File dataPath,
                                  File resultsPath,
                                  int scenarioNumber) {
        HugeGraphCoreSingleInsertion insertion =
                new HugeGraphCoreSingleInsertion(this.graph, resultsPath);
        insertion.createGraph(dataPath, scenarioNumber);
    }

    /* FN, FA, FS and CW */
    @Override
    public void open() {
        buildGraphEnv(false);
    }

    @Override
    public void shutdown() {
    }

    /* Delete */
    @Override
    public void delete() {
    }

    private void buildGraphEnv(boolean clear) {
        this.graph = ExampleUtil.loadGraph(clear, false);
        SchemaManager schema = this.graph.schema();

        schema.propertyKey(COMMUNITY).asInt().ifNotExist().create();
        schema.propertyKey(NODE_COMMUNITY).asInt().ifNotExist().create();
        schema.vertexLabel(NODE)
              .properties(COMMUNITY, NODE_COMMUNITY)
              .nullableKeys(COMMUNITY, NODE_COMMUNITY)
              .useCustomizeNumberId().ifNotExist().create();
        schema.edgeLabel(SIMILAR).link(NODE, NODE).ifNotExist().create();
        schema.indexLabel("nodeByCommunity")
              .onV(NODE).by(COMMUNITY).ifNotExist().create();
        schema.indexLabel("nodeByNodeCommunity")
              .onV(NODE).by(NODE_COMMUNITY).ifNotExist().create();
    }

    @Override
    public void shortestPath(HugeVertex fromNode, Integer node) {
        LOG.debug(">>>>>" + counter++ + " round,(from node: "
                  + fromNode.id() + ", to node: " + node + ")");
        Object result = this.graph.traversal().V(fromNode.id())
                            .repeat(__.out().simplePath())
                            .until(__.hasId(node).or().loops().is(P.gte(3)))
                            .hasId(node).path().limit(1).toList();
        LOG.debug(result);
    }

    @Override
    public int getNodeCount() {
        return this.graph.traversal().V().hasLabel(NODE).count()
                   .next().intValue();
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId) {
        Set<Integer> neighbors = new HashSet<>();
        List<Vertex> vertices = this.graph.traversal().V(nodeId).out(SIMILAR)
                                    .toList();
        for (Vertex v : vertices) {
            neighbors.add(((Number) v.id()).intValue());
        }
        return neighbors;
    }

    @Override
    public double getNodeWeight(int nodeId) {
        return this.graph.traversal().V(nodeId).out().count().next();
    }

    @Override
    public void initCommunityProperty() {
        LOG.debug("Init community property");
        int communityCounter = 0;
        List<Vertex> vertices = this.graph.traversal().V().hasLabel(NODE)
                                    .toList();
        for (Vertex v : vertices) {
            v.property(NODE_COMMUNITY, communityCounter);
            v.property(COMMUNITY, communityCounter);
            communityCounter++;
            if (communityCounter % 500 == 0) {
                this.graph.tx().commit();
            }
        }
        LOG.debug("Initial community number is: " + communityCounter);
        this.graph.tx().commit();
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(
                        int nodeCommunities) {
        Set<Integer> communities = new HashSet<>();
        List<Vertex> vertices = this.graph.traversal().V()
                                    .has(NODE_COMMUNITY, nodeCommunities)
                                    .toList();
        for (Vertex v : vertices) {
            List<Vertex> neighbors = this.graph.traversal().V(v.id())
                                         .out(SIMILAR).toList();
            for (Vertex neighbor : neighbors) {
                int community = (Integer) neighbor.property(COMMUNITY).value();
                communities.add(community);
            }
        }
        return communities;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community) {
        Set<Integer> nodes = new HashSet<>();
        List<Vertex> vertices = this.graph.traversal().V()
                                    .has(COMMUNITY, community).toList();
        for (Vertex v : vertices) {
            nodes.add(((Number) v.id()).intValue());
        }
        return nodes;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
        Set<Integer> nodes = new HashSet<>();
        List<Vertex> vertices = this.graph.traversal().V()
                                    .has(NODE_COMMUNITY, nodeCommunity)
                                    .toList();
        for (Vertex v : vertices) {
            nodes.add(((Number) v.id()).intValue());
        }
        return nodes;
    }

    @Override
    public double getEdgesInsideCommunity(int nodeCommunity,
                                          int communityNodes) {
        double edges = 0;
        List<Vertex> vertices = this.graph.traversal().V()
                                    .has(NODE_COMMUNITY, nodeCommunity)
                                    .toList();
        List<Vertex> commVertices = this.graph.traversal().V()
                                        .has(COMMUNITY, communityNodes)
                                        .toList();
        for (Vertex v : vertices) {
            List<Vertex> neighbors = this.graph.traversal().V(v.id())
                                         .out(SIMILAR).toList();
            for (Vertex n : neighbors) {
                if (commVertices.contains(n)) {
                    edges++;
                }
            }
        }
        return edges;
    }

    @Override
    public double getCommunityWeight(int community) {
        double communityWeight = 0;
        List<Vertex> vertices = this.graph.traversal().V()
                                    .has(COMMUNITY, community).toList();
        for (Vertex vertex : vertices) {
            communityWeight += getNodeOutDegree(vertex);
        }
        return communityWeight;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCom) {

        double nodeCommunityWeight = 0;
        List<Vertex> vertices = this.graph.traversal().V()
                                    .has(NODE_COMMUNITY, nodeCom).toList();
        for (Vertex v : vertices) {
            nodeCommunityWeight += getNodeOutDegree(v);
        }
        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int from, int to) {
        List<Vertex> vertices = this.graph.traversal().V()
                                    .has(NODE_COMMUNITY, from).toList();
        int count = 0;
        for (Vertex v : vertices) {
            v.property(COMMUNITY, to);
            count++;
            if (count % 500 == 0) {
                this.graph.tx().commit();
            }
        }
        this.graph.tx().commit();
    }

    @Override
    public double getGraphWeightSum() {
        return this.graph.traversal().E().count().next();
    }

    @Override
    public int reInitializeCommunities() {
        LOG.debug("ReInitialize communities");
        Map<Integer, Integer> initCommunities = new HashMap<>();
        int communityCounter = 0;
        int count = 0;
        for (Vertex v : this.graph.traversal().V().hasLabel(NODE).toList()) {
            int communityId = (int) v.property(COMMUNITY).value();
            if (!initCommunities.containsKey(communityId)) {
                initCommunities.put(communityId, communityCounter);
                communityCounter++;
            }
            int newCommunityId = initCommunities.get(communityId);
            v.property(COMMUNITY, newCommunityId);
            v.property(NODE_COMMUNITY, newCommunityId);
            count++;
            if (count % 500 == 0) {
                this.graph.tx().commit();
            }
        }
        this.graph.tx().commit();
        LOG.debug("Community number is: " + communityCounter + " now");
        return communityCounter;
    }

    @Override
    public int getCommunityFromNode(int nodeId) {
        Vertex vertex = this.graph.traversal().V(nodeId).next();
        return (int) vertex.property(COMMUNITY).value();
    }

    @Override
    public int getCommunity(int nodeCommunity) {
        Vertex vertex = this.graph.traversal().V()
                            .has(NODE_COMMUNITY, nodeCommunity).next();
        return (int) vertex.property(COMMUNITY).value();
    }

    @Override
    public int getCommunitySize(int community) {
        Set<Integer> nodeCommunities = new HashSet<>();
        List<Vertex> vertices = this.graph.traversal().V()
                                    .has(COMMUNITY, community).toList();
        for (Vertex v : vertices) {
            int nodeCommunity = (int) v.property(NODE_COMMUNITY).value();
            if (!nodeCommunities.contains(nodeCommunity)) {
                nodeCommunities.add(nodeCommunity);
            }
        }
        return nodeCommunities.size();
    }

    @Override
    public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
        Map<Integer, List<Integer>> communities = new HashMap<>();
        for (int i = 0; i < numberOfCommunities; i++) {
            List<Integer> vs = new ArrayList<>();
            List<Vertex> vertices = this.graph.traversal().V()
                                        .has(COMMUNITY, i).toList();
            for (Vertex v : vertices) {
                vs.add(((Number) v.id()).intValue());
            }
            communities.put(i, vs);
        }
        return communities;
    }

    @Override
    public boolean nodeExists(int nodeId) {
        return this.graph.traversal().V(nodeId).hasNext();
    }

    public double getNodeOutDegree(Vertex vertex) {
        return getNodeDegree(vertex, Direction.OUT);
    }

    public double getNodeDegree(Vertex vertex, Direction direction) {
        switch (direction) {
            case IN:
                return this.graph.traversal().V(vertex.id()).in(SIMILAR)
                                 .count().next();
            case OUT:
                return this.graph.traversal().V(vertex.id()).out(SIMILAR)
                                 .count().next();
            case BOTH:
            default:
                throw new AssertionError(String.format(
                          "Only support IN or OUT, but got: '%s'", direction));
        }
    }
}
