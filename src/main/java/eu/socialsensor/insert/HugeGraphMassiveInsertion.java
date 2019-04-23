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

package eu.socialsensor.insert;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;

import eu.socialsensor.graphdatabases.HugeGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;

public class HugeGraphMassiveInsertion extends InsertionBase<Integer> {

    private ExecutorService pool = Executors.newFixedThreadPool(8);
    private Set<Integer> vertices = new HashSet<>();

    private static final int VERTEX_BATCH_NUMBER = 500;
    private static final int EDGE_BATCH_NUMBER = 500;

    private List<Vertex> vertexList = new ArrayList<>(VERTEX_BATCH_NUMBER);
    private List<Edge> edgeList = new ArrayList<>(EDGE_BATCH_NUMBER);

    private final GraphManager graphManager;

    public HugeGraphMassiveInsertion(GraphManager graphManager) {
        super(GraphDatabaseType.HUGEGRAPH, null);
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
        edge.sourceId(src);
        edge.sourceLabel(HugeGraphDatabase.NODE);
        edge.targetId(dest);
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
