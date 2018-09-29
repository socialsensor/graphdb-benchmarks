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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.dist.RegisterUtil;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.traversal.optimize.HugeTraverser;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;

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
    private static final String CONF = "hugegraph.properties";
    private static boolean registered = false;

    private static final String NODE = "node";
    private static final int BATCH_SIZE = 500;
    private static int counter = 0;

    public HugeGraphCoreDatabase(BenchmarkConfiguration config,
                                 File dbStorageDirectoryIn) {
        super(GraphDatabaseType.HUGEGRAPH_CORE, dbStorageDirectoryIn);
    }

    @Override
    public HugeVertex getOtherVertexFromEdge(HugeEdge edge,
                                             HugeVertex oneVertex) {
        if (edge.sourceVertex().equals(oneVertex)) {
            return edge.targetVertex();
        } else {
            assert edge.targetVertex().equals(oneVertex);
            return edge.sourceVertex();
        }
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
        Query query = new Query(HugeType.EDGE);
        query.capacity(Query.NO_CAPACITY);
        return this.graph.edges(query);
    }

    @Override
    public Iterator<Edge> getNeighborsOfVertex(HugeVertex v) {
        return v.edges(Direction.BOTH, SIMILAR);
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
        CloseableIterator.closeIterator(it);
    }

    @Override
    public Iterator<Vertex> getVertexIterator() {
        Query query = new Query(HugeType.VERTEX);
        query.capacity(Query.NO_CAPACITY);
        return this.graph.vertices(query);
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
        CloseableIterator.closeIterator(it);
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
        this.graph = loadGraph(clear);
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
        HugeTraverser traverser = new HugeTraverser(this.graph);
        List<Id> path = traverser.shortestPath(fromNode.id(),
                                               IdGenerator.of(node.longValue()),
                                               Directions.OUT, SIMILAR, 5,
                                               -1, -1);
        LOG.debug(path);
    }

    @Override
    public int getNodeCount() {
        return this.graph.traversal().V().count().next().intValue();
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId) {
        Set<Integer> neighbors = new HashSet<>();
        Iterator<Vertex> vertices = this.graph.traversal().V(nodeId)
                                              .outE(SIMILAR).otherV();
        while (vertices.hasNext()) {
            neighbors.add(((Number) vertices.next().id()).intValue());
        }
        return neighbors;
    }

    @Override
    public double getNodeWeight(int nodeId) {
        return this.getNodeDegree(nodeId, Direction.OUT);
    }

    @Override
    public void initCommunityProperty() {
        LOG.debug("Init community property");
        int communityCounter = 0;
        Iterator<Vertex> vertices = this.graph.traversal().V();
        while (vertices.hasNext()) {
            Vertex v = vertices.next();
            v.property(NODE_COMMUNITY, communityCounter);
            v.property(COMMUNITY, communityCounter);
            communityCounter++;
            if (communityCounter % BATCH_SIZE == 0) {
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
        Iterator<Vertex> vertices = this.vertices(NODE_COMMUNITY,
                                                  nodeCommunities);
        while (vertices.hasNext()) {
            Vertex v = vertices.next();
            Iterator<Vertex> neighbors = this.graph.traversal().V(v.id())
                                             .out(SIMILAR);
            while (neighbors.hasNext()) {
                Vertex neighbor = neighbors.next();
                int community = (Integer) neighbor.property(COMMUNITY).value();
                communities.add(community);
            }
        }
        return communities;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community) {
        Set<Integer> nodes = new HashSet<>();
        Iterator<Vertex> vertices = this.vertices(COMMUNITY, community);
        while (vertices.hasNext()) {
            Vertex v = vertices.next();
            nodes.add(((Number) v.id()).intValue());
        }
        return nodes;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
        Set<Integer> nodes = new HashSet<>();
        Iterator<Vertex> vertices = this.vertices(NODE_COMMUNITY,
                                                  nodeCommunity);
        while (vertices.hasNext()) {
            Vertex v = vertices.next();
            nodes.add(((Number) v.id()).intValue());
        }
        return nodes;
    }

    @Override
    public double getEdgesInsideCommunity(int nodeCommunity,
                                          int communityNodes) {
        double edges = 0;
        Iterator<Vertex> vertices = this.vertices(NODE_COMMUNITY,
                                                  nodeCommunity);
        Set<Vertex> commVertices = new HashSet<>();
        IteratorUtils.fill(this.vertices(COMMUNITY, communityNodes),
                           commVertices);
        while (vertices.hasNext()) {
            Iterator<Vertex> neighbors = this.graph.traversal()
                                             .V(vertices.next().id())
                                             .out(SIMILAR);
            while (neighbors.hasNext()) {
                if (commVertices.contains(neighbors.next())) {
                    edges++;
                }
            }
        }
        return edges;
    }

    @Override
    public double getCommunityWeight(int community) {
        double communityWeight = 0;
        Iterator<Vertex> vertices = this.vertices(COMMUNITY, community);
        while (vertices.hasNext()) {
            communityWeight += this.getNodeOutDegree(vertices.next());
        }
        return communityWeight;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCom) {

        double nodeCommunityWeight = 0;
        Iterator<Vertex> vertices = this.vertices(NODE_COMMUNITY, nodeCom);
        while (vertices.hasNext()) {
            nodeCommunityWeight += this.getNodeOutDegree(vertices.next());
        }
        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int from, int to) {
        Iterator<Vertex> vertices = this.vertices(NODE_COMMUNITY, from);
        int count = 0;
        while (vertices.hasNext()) {
            vertices.next().property(COMMUNITY, to);
            count++;
            if (count % BATCH_SIZE == 0) {
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
        Iterator<Vertex> vertices = this.graph.traversal().V();
        while (vertices.hasNext()) {
            Vertex v = vertices.next();
            int communityId = (int) v.property(COMMUNITY).value();
            if (!initCommunities.containsKey(communityId)) {
                initCommunities.put(communityId, communityCounter);
                communityCounter++;
            }
            int newCommunityId = initCommunities.get(communityId);
            v.property(COMMUNITY, newCommunityId);
            v.property(NODE_COMMUNITY, newCommunityId);
            count++;
            if (count % BATCH_SIZE == 0) {
                this.graph.tx().commit();
            }
        }
        this.graph.tx().commit();
        LOG.debug("Community number is: " + communityCounter + " now");
        return communityCounter;
    }

    @Override
    public int getCommunityFromNode(int nodeId) {
        Vertex vertex = this.graph.vertices(nodeId).next();
        return (int) vertex.property(COMMUNITY).value();
    }

    @Override
    public int getCommunity(int nodeCommunity) {
        Vertex vertex = this.vertices(NODE_COMMUNITY, nodeCommunity).next();
        return (int) vertex.property(COMMUNITY).value();
    }

    @Override
    public int getCommunitySize(int community) {
        Set<Integer> nodeCommunities = new HashSet<>();
        Iterator<Vertex> vertices = this.vertices(COMMUNITY, community);
        while (vertices.hasNext()) {
            Vertex v = vertices.next();
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
            Iterator<Vertex> vertices = this.vertices(COMMUNITY, i);
            while (vertices.hasNext()) {
                vs.add(((Number) vertices.next().id()).intValue());
            }
            communities.put(i, vs);
        }
        return communities;
    }

    @Override
    public boolean nodeExists(int nodeId) {
        return this.graph.vertices(nodeId).hasNext();
    }

    private long getNodeOutDegree(Vertex vertex) {
        return getNodeDegree(vertex.id(), Direction.OUT);
    }

    private long getNodeDegree(Object id, Direction direction) {
        switch (direction) {
            case IN:
                return this.graph.traversal().V(id).inE(SIMILAR)
                                 .count().next();
            case OUT:
                return this.graph.traversal().V(id).outE(SIMILAR)
                                 .count().next();
            case BOTH:
            default:
                throw new AssertionError(String.format(
                          "Only support IN or OUT, but got: '%s'", direction));
        }
    }

    private Iterator<Vertex> vertices(String prop, Object value) {
        return this.graph.traversal().V().has(prop, value);
    }

    private static HugeGraph loadGraph(boolean needClear) {
        // Register backends if needed
        if (!registered) {
            RegisterUtil.registerBackends();
            registered = true;
        }

        String conf = CONF;
        try {
            String path = HugeGraphCoreDatabase.class.getClassLoader()
                                               .getResource(CONF).getPath();
            File file = new File(path);
            if (file.exists() && file.isFile()) {
                conf = path;
            }
        } catch (Exception ignored) {
        }
        // Open graph using configuration file
        HugeGraph graph = HugeFactory.open(conf);

        // Clear graph if needed
        if (needClear) {
            graph.clearBackend();
        }
        // Init backend
        graph.initBackend();

        return graph;
    }
}
