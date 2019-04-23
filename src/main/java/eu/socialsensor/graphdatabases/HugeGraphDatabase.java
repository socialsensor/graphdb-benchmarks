/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.baidu.hugegraph.driver.GremlinManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Graph;
import com.baidu.hugegraph.structure.graph.Graph.HugeEdge;
import com.baidu.hugegraph.structure.graph.Graph.HugeVertex;
import com.baidu.hugegraph.structure.graph.Path;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.structure.gremlin.Result;
import com.baidu.hugegraph.structure.gremlin.ResultSet;
import com.baidu.hugegraph.structure.constant.Direction;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import eu.socialsensor.insert.HugeGraphMassiveInsertion;
import eu.socialsensor.insert.HugeGraphSingleInsertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;

public class HugeGraphDatabase extends GraphDatabaseBase<
             Iterator<HugeVertex>,
             Iterator<HugeEdge>,
             HugeVertex,
             HugeEdge> {

    private static final Logger LOG = LogManager.getLogger();

    private HugeClient hugeClient = null;
    private GremlinManager gremlin = null;
    private final BenchmarkConfiguration conf;

    public static final String NODE = "node";
    private static int counter = 0;

    private static final int CLIENT_TIMEOUT = 600;

    public HugeGraphDatabase(BenchmarkConfiguration config,
                             File dbStorageDirectoryIn) {
        super(GraphDatabaseType.HUGEGRAPH, dbStorageDirectoryIn);
        this.conf = config;
    }

    @Override
    public HugeVertex getOtherVertexFromEdge(HugeEdge edge,
                                             HugeVertex oneVertex) {
        return edge.other(oneVertex);
    }

    @Override
    public HugeVertex getSrcVertexFromEdge(HugeEdge edge) {
        return edge.source();
    }

    @Override
    public HugeVertex getDestVertexFromEdge(HugeEdge edge) {
        return edge.target();
    }

    @Override
    public HugeVertex getVertex(Integer i) {
        return new HugeVertex(this.hugeClient.graph().getVertex(i));
    }

    @Override
    public Iterator<HugeEdge> getAllEdges() {
        return new Graph(this.hugeClient.graph()).edges();
    }

    @Override
    public Iterator<HugeEdge> getNeighborsOfVertex(HugeVertex v) {
        return v.getEdges().iterator();
    }

    @Override
    public boolean edgeIteratorHasNext(Iterator<HugeEdge> it) {
        return it.hasNext();
    }

    @Override
    public HugeEdge nextEdge(Iterator<HugeEdge> it) {
        return it.next();
    }

    @Override
    public void cleanupEdgeIterator(Iterator<HugeEdge> it) {
    }

    @Override
    public Iterator<HugeVertex> getVertexIterator() {
        return new Graph(this.hugeClient.graph()).vertices();
    }

    @Override
    public boolean vertexIteratorHasNext(Iterator<HugeVertex> it) {
        return it.hasNext();
    }

    @Override
    public HugeVertex nextVertex(Iterator<HugeVertex> it) {
        return it.next();
    }

    @Override
    public void cleanupVertexIterator(Iterator<HugeVertex> it) {
    }

    /* Massive insertion */
    @Override
    public void createGraphForMassiveLoad() {
        buildGraphEnv(true);
    }

    @Override
    public void massiveModeLoading(File dataPath) {
        HugeGraphMassiveInsertion insertion =
                new HugeGraphMassiveInsertion(this.hugeClient.graph());
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
        HugeGraphSingleInsertion insertion = new HugeGraphSingleInsertion(
                this.hugeClient.graph(), resultsPath);
        insertion.createGraph(dataPath, scenarioNumber);
    }

    /* FN, FA, FS or CW */
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
        this.hugeClient = new HugeClient(this.conf.getHugegraphUrl(),
                                         this.conf.getHugegraphGraph(),
                                         CLIENT_TIMEOUT);
        this.gremlin = this.hugeClient.gremlin();
        SchemaManager schema = this.hugeClient.schema();

        if (clear) {
            this.clearAll();
        }
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

    private void clearAll() {
        // Clear edge
        this.hugeClient.graph().listEdges().forEach(edge -> {
            this.hugeClient.graph().removeVertex(edge);
        });
        // Clear vertex
        this.hugeClient.graph().listVertices().forEach(vertex -> {
            this.hugeClient.graph().removeVertex(vertex);
        });
        // Clear schema
        this.hugeClient.schema().getIndexLabels().forEach(indexLabel -> {
            this.hugeClient.schema().removeIndexLabel(indexLabel.name());
        });
        this.hugeClient.schema().getEdgeLabels().forEach(edgeLabel -> {
            this.hugeClient.schema().removeEdgeLabel(edgeLabel.name());
        });
        this.hugeClient.schema().getVertexLabels().forEach(vertexLabel -> {
            this.hugeClient.schema().removeVertexLabel(vertexLabel.name());
        });
        this.hugeClient.schema().getPropertyKeys().forEach(propertyKey -> {
            this.hugeClient.schema().removePropertyKey(propertyKey.name());
        });
    }

    @Override
    public void shortestPath(HugeVertex fromNode, Integer node) {
        LOG.debug(">>>>>" + counter++ + " round,(from node: " +
                  fromNode.vertex().id() + ", to node: " + node + ")");
        String query = String.format("g.V(%s).repeat(out().simplePath())" +
                                     ".until(hasId(%s).or().loops().is(gte(3)" +
                                     ")).hasId(%s).path().limit(1)",
                                     fromNode.vertex().id(), node, node);
        ResultSet resultSet = this.gremlin.gremlin(query).execute();

        Iterator<Result> results = resultSet.iterator();
        results.forEachRemaining(result -> {
            LOG.debug(result.getObject().getClass());
            Object object = result.getObject();
            if (object instanceof Vertex) {
                LOG.debug(((Vertex) object).id());
            } else if (object instanceof HugeEdge) {
                LOG.debug(((HugeEdge) object).edge().id());
            } else if (object instanceof Path) {
                List<Object> elements = ((Path) object).objects();
                elements.forEach(element -> {
                    LOG.debug(element.getClass());
                    LOG.debug(element);
                });
            } else {
                LOG.debug(object);
            }
        });
    }

    @Override
    public int getNodeCount() {
        String query = "g.V().hasLabel('node').count()";
        ResultSet resultSet = this.gremlin.gremlin(query).execute();
        return ((Number) resultSet.iterator().next().getObject()).intValue();
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId) {
        Set<Integer> neighbors = new HashSet<>();
        List<Edge> edges = this.hugeClient.graph().getEdges(nodeId,
                                                            Direction.OUT);
        for (Edge e : edges) {
            neighbors.add((Integer) e.targetId());
        }
        return neighbors;
    }

    @Override
    public double getNodeWeight(int nodeId) {
        Vertex vertex = this.hugeClient.graph().getVertex(nodeId);
        return getNodeOutDegree(vertex);
    }

    @Override
    public void initCommunityProperty() {
        LOG.debug("Init community property");
        int communityCounter = 0;
        for (Vertex v : this.hugeClient.graph().listVertices("node")) {
            v.property(NODE_COMMUNITY, communityCounter);
            v.property(COMMUNITY, communityCounter);
            communityCounter++;
        }
        LOG.debug("Initial community number is: " + communityCounter);
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(
                        int nodeCommunities) {
        Set<Integer> communities = new HashSet<>();
        Iterable<Vertex> vertices = getVerticesByProperty(NODE_COMMUNITY,
                                                          nodeCommunities);
        for (Vertex v : vertices) {
            for (Vertex neighbor : getVertices(v, Direction.OUT, SIMILAR)) {
                int community = (Integer) neighbor.properties().get(COMMUNITY);
                communities.add(community);
            }
        }
        return communities;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community) {
        Set<Integer> nodes = new HashSet<>();
        for (Vertex v : getVerticesByProperty(COMMUNITY, community)) {
            nodes.add((Integer) v.id());
        }
        return nodes;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
        Set<Integer> nodes = new HashSet<>();
        for (Vertex v : getVerticesByProperty(NODE_COMMUNITY, nodeCommunity)) {
            nodes.add((Integer) v.id());
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
        Iterable<Vertex> vertices = getVerticesByProperty(COMMUNITY, community);
        if (Iterables.size(vertices) > 1) {
            for (Vertex vertex : vertices) {
                communityWeight += getNodeOutDegree(vertex);
            }
        }
        return communityWeight;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCom) {

        double nodeCommunityWeight = 0;
        for (Vertex v : getVerticesByProperty(NODE_COMMUNITY, nodeCom)) {
            nodeCommunityWeight += getNodeOutDegree(v);
        }
        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int from, int to) {
        Iterable<Vertex> vertices = getVerticesByProperty(NODE_COMMUNITY, from);
        for (Vertex v : vertices) {
            v.property(COMMUNITY, to);
        }
    }

    @Override
    public double getGraphWeightSum() {
        String query = "g.E().count()";
        ResultSet resultSet = this.gremlin.gremlin(query).execute();
        return ((Number) resultSet.iterator().next().getObject()).doubleValue();
    }

    @Override
    public int reInitializeCommunities() {
        LOG.debug("ReInitialize communities");
        Map<Integer, Integer> initCommunities = new HashMap<>();
        int communityCounter = 0;
        for (Vertex v : this.hugeClient.graph().listVertices(NODE)) {
            int communityId = (int) v.properties().get(COMMUNITY);
            if (!initCommunities.containsKey(communityId)) {
                initCommunities.put(communityId, communityCounter);
                communityCounter++;
            }
            int newCommunityId = initCommunities.get(communityId);
            v.property(COMMUNITY, newCommunityId);
            v.property(NODE_COMMUNITY, newCommunityId);
        }
        LOG.debug("Community number is: " + communityCounter + " now");
        return communityCounter;
    }

    @Override
    public int getCommunityFromNode(int nodeId) {
        Vertex vertex = this.hugeClient.graph().getVertex(nodeId);
        return (Integer) vertex.properties().get(COMMUNITY);
    }

    @Override
    public int getCommunity(int nodeCommunity) {
        Vertex vertex = getVerticesByProperty(NODE_COMMUNITY, nodeCommunity)
                        .iterator().next();
        return (Integer) vertex.properties().get(COMMUNITY);
    }

    @Override
    public int getCommunitySize(int community) {
        Set<Integer> nodeCommunities = new HashSet<>();
        for (Vertex v : getVerticesByProperty(COMMUNITY, community)) {
            int nodeCommunity = (Integer) v.properties().get(NODE_COMMUNITY);
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
            List<Integer> vertices = new ArrayList<>();
            for (Vertex v : getVerticesByProperty(COMMUNITY, i)) {
                vertices.add((Integer) v.id());
            }
            communities.put(i, vertices);
        }
        return communities;
    }

    @Override
    public boolean nodeExists(int nodeId) {
        Vertex vertex = this.hugeClient.graph().getVertex(nodeId);
        return vertex != null;
    }

    private Iterable<Vertex> getVerticesByProperty(String pKey, int pValue) {
        return this.hugeClient.graph()
                   .listVertices("node", ImmutableMap.of(pKey, pValue));
    }

    public double getNodeOutDegree(Vertex vertex) {
        return getNodeDegree(vertex, Direction.OUT);
    }

    public double getNodeInDegree(Vertex vertex) {
        return getNodeDegree(vertex, Direction.IN);
    }

    public double getNodeDegree(Vertex vertex, Direction direction) {
        String direct = direction.equals(Direction.OUT) ? "out" : "in";
        String query = String.format("g.V('%s').%s('%s').count()",
                                     vertex.id(), direct, SIMILAR);
        ResultSet resultSet = this.gremlin.gremlin(query).execute();
        Iterator<Result> it = resultSet.iterator();
        return ((Number) it.next().getObject()).doubleValue();
    }

    private Iterable<Vertex> getVertices(Vertex vertex, Direction direct,
                                         String edgetype) {
        String vertexId = vertex == null ? "" : vertex.id().toString();
        String direction = direct.equals(Direction.OUT) ? "out" : "in";
        String query = String.format("g.V('%s').%s('%s')",
                                     vertexId, direction, SIMILAR);
        ResultSet resultSet = this.gremlin.gremlin(query).execute();
        Iterator<Result> it = resultSet.iterator();
        List<Vertex> vertices = new LinkedList<>();
        while (it.hasNext()) {
            vertices.add(it.next().getVertex());
        }
        return vertices;
    }
}
