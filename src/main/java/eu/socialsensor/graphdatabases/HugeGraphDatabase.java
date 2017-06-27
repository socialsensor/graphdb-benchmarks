/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package eu.socialsensor.graphdatabases;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;
//import com.tinkerpop.blueprints.Edge;
//import com.tinkerpop.blueprints.Vertex;

import eu.socialsensor.insert.HugeGraphMassiveInsertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.HugeGraphUtils;

/**
 * Created by zhangsuochao on 17/6/21.
 */
public class HugeGraphDatabase extends GraphDatabaseBase<Iterator<Vertex>, Iterator<Edge>, Vertex, Edge> {

    protected final HugeClient hugeClient;

    public HugeGraphDatabase(BenchmarkConfiguration config, File dbStorageDirectoryIn) {
        super(GraphDatabaseType.HUGEGRAPH_CASSANDRA, dbStorageDirectoryIn);
        hugeClient = HugeClient.open("http://localhost:8080",
                "hugegraph");
    }

    @Override
    public Vertex getOtherVertexFromEdge(Edge r, Vertex oneVertex) {
        return null;
    }

    @Override
    public Vertex getSrcVertexFromEdge(Edge edge) {
        return null;
    }

    @Override
    public Vertex getDestVertexFromEdge(Edge edge) {
        return null;
    }

    @Override
    public Vertex getVertex(Integer i) {
        return hugeClient.graph().getVertex(HugeGraphUtils.createId("node",i+""));
    }

    @Override
    public Iterator<Edge> getAllEdges() {
        return hugeClient.graph().getEdges().iterator();
    }

    @Override
    public Iterator<Edge> getNeighborsOfVertex(Vertex v) {
        return null;
    }

    @Override
    public boolean edgeIteratorHasNext(Iterator<Edge> it) {
        return false;
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
        return null;
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

    }

    @Override
    public void createGraphForSingleLoad() {

    }

    @Override
    public void massiveModeLoading(File dataPath) {
        SchemaManager schemaManager = hugeClient.schema();
        schemaManager.makePropertyKey("nodeId").asText().ifNotExist().create();
        schemaManager.makeVertexLabel("node").properties("nodeId").primaryKeys("nodeId").ifNotExist().create();
        schemaManager.makeEdgeLabel("link").link("node","node").ifNotExist().create();
        HugeGraphMassiveInsertion massiveInsertion = new HugeGraphMassiveInsertion(this.hugeClient.graph());
        massiveInsertion.createGraph(dataPath,0 /* scenarioNumber */);
    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber) {

    }

    @Override
    public void createGraphForMassiveLoad() {

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

    @Override
    public void shortestPath(Vertex fromNode, Integer node) {

    }

    @Override
    public int getNodeCount() {
        return 0;
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId) {
        return null;
    }

    @Override
    public double getNodeWeight(int nodeId) {
        return 0;
    }

    @Override
    public void initCommunityProperty() {

    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
        return null;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community) {
        return null;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
        return null;
    }

    @Override
    public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes) {
        return 0;
    }

    @Override
    public double getCommunityWeight(int community) {
        return 0;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity) {
        return 0;
    }

    @Override
    public void moveNode(int from, int to) {

    }

    @Override
    public double getGraphWeightSum() {
        return 0;
    }

    @Override
    public int reInitializeCommunities() {
        return 0;
    }

    @Override
    public int getCommunityFromNode(int nodeId) {
        return 0;
    }

    @Override
    public int getCommunity(int nodeCommunity) {
        return 0;
    }

    @Override
    public int getCommunitySize(int community) {
        return 0;
    }

    @Override
    public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
        return null;
    }

    @Override
    public boolean nodeExists(int nodeId) {
        return false;
    }
}
