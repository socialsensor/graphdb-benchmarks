/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package eu.socialsensor.insert;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;

import eu.socialsensor.graphdatabases.HugeGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.HugeGraphUtils;

public class HugeGraphMassiveInsertion extends InsertionBase<Integer> {

    private ExecutorService pool = Executors.newFixedThreadPool(8);
    private Set<Integer> vertices = new HashSet<>();

    private static final int VERTEX_BATCH_NUMBER = 500;
    private static final int EDGE_BATCH_NUMBER = 500;

    private List<Vertex> vertexList = new ArrayList<>(VERTEX_BATCH_NUMBER);
    private List<Edge> edgeList = new ArrayList<>(EDGE_BATCH_NUMBER);

    private final GraphManager graphManager;

    public HugeGraphMassiveInsertion(GraphManager graphManager) {
        super(GraphDatabaseType.HUGEGRAPH_CASSANDRA, null);
        this.graphManager = graphManager;
    }

    @Override
    protected Integer getOrCreate(String value) {
        Integer v = Integer.valueOf(value);
        if (!this.vertices.contains(v)) {
            this.vertices.add(v);
            Vertex vertex = new Vertex(HugeGraphDatabase.NODE);
            vertex.id(v);
            this.vertexList.add(vertex);
        }

        if (this.vertexList.size() >= VERTEX_BATCH_NUMBER) {
            batchcommitVertex();
        }
        return v;
    }

    @Override
    protected void relateNodes(Integer src, Integer dest) {
        Edge edge = new Edge(HugeGraphDatabase.SIMILAR);
        edge.source(src);
        edge.sourceLabel(HugeGraphDatabase.NODE);
        edge.target(dest);
        edge.targetLabel(HugeGraphDatabase.NODE);

        this.edgeList.add(edge);
        if (this.edgeList.size() >= EDGE_BATCH_NUMBER) {
            batchcommitEdge();
        }
    }

    public void batchcommitVertex() {
        List<Vertex> list = this.vertexList;
        this.vertexList = new ArrayList<>(VERTEX_BATCH_NUMBER);
        this.pool.submit(() -> {
            this.graphManager.addVertices(list);
        });
    }

    public void batchcommitEdge() {
        List<Edge> list = this.edgeList;
        this.edgeList = new ArrayList<>(EDGE_BATCH_NUMBER);
        this.pool.submit(() -> {
            this.graphManager.addEdges(list, false);
        });
    }

    @Override
    protected void post() {
        if (this.vertexList.size() > 0) {
            batchcommitVertex();
        }
        if (this.edgeList.size() > 0) {
            batchcommitEdge();
        }
        this.pool.shutdown();
        try {
            this.pool.awaitTermination(3, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
