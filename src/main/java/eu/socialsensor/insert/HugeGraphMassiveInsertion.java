/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package eu.socialsensor.insert;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import eu.socialsensor.graphdatabases.HugeGraphDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;

import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.HugeGraphUtils;

/**
 * Created by zhangsuochao on 17/6/21.
 */
public class HugeGraphMassiveInsertion extends InsertionBase<Integer> {

    private static final Logger LOG = LogManager.getLogger();
    private final GraphManager graphManager;
    private Set<Integer> vertices = new HashSet<>();
    private static final int VERTEXBATCHNUMBER = 500;
    private static final int EDGEBATCHNUMBER = 500;

    private List<Vertex> vertexList = new LinkedList<>();
    private List<Edge> edgeList = new LinkedList<>();
    ExecutorService pool;
    public HugeGraphMassiveInsertion(GraphManager graphManager) {
        super(GraphDatabaseType.HUGEGRAPH_CASSANDRA, null);
        this.graphManager = graphManager;
        pool = Executors.newCachedThreadPool();
    }

    @Override
    protected Integer getOrCreate(String value) {
        Vertex vertex;
        Integer v = Integer.valueOf(value);
        if (!vertices.contains(v)) {
            vertices.add(v);
            vertex = new Vertex(HugeGraphDatabase.NODE)
                    .property(HugeGraphDatabase.NODE_ID, value);
            vertexList.add(vertex);
        }

        if (vertexList.size() >= VERTEXBATCHNUMBER) {
            batchcommitVertex();
        }
        return v;
    }

    @Override
    protected void relateNodes(Integer src, Integer dest) {
        Edge edge = new Edge(HugeGraphDatabase.SIMILAR);
        String srcId = HugeGraphUtils.createId(HugeGraphDatabase.NODE,
                src.toString());
        edge.source(srcId);
        edge.sourceLabel(HugeGraphDatabase.NODE);
        String destId = HugeGraphUtils.createId(HugeGraphDatabase.NODE,
                dest.toString());
        edge.target(destId);
        edge.targetLabel(HugeGraphDatabase.NODE);

        edgeList.add(edge);
        if (edgeList.size() >= EDGEBATCHNUMBER) {
            batchcommitEdge();
        }
    }

    List<Thread> threads = new LinkedList<>();

    public void batchcommitVertex() {
        List<Vertex> list = vertexList;
        vertexList = new LinkedList<>();
        pool.submit(() -> {
            graphManager.addVertices(list);
        });
    }

    public void batchcommitEdge() {
        List<Edge> list = edgeList;
        edgeList = new LinkedList<>();
        pool.submit(() -> {
            graphManager.addEdges(list, false);
        });
    }

    @Override
    protected void post() {
        if (vertexList.size() > 0) {
            batchcommitVertex();
        }
        if (edgeList.size() > 0) {
            batchcommitEdge();
        }
        pool.shutdown();
        try {
            pool.awaitTermination(3, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
