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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.T;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;

import eu.socialsensor.graphdatabases.HugeGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;

public class HugeGraphCoreMassiveInsertion extends InsertionBase<Integer> {

    private static final int EDGE_BATCH_NUMBER = 500;

    private ExecutorService pool = Executors.newFixedThreadPool(8);

    private Set<Integer> allVertices = new HashSet<>();

    private List<Integer> vertices;
    private List<Pair<Integer, Integer>> edges;

    private final HugeGraph graph;
    private final VertexLabel vl;

    public HugeGraphCoreMassiveInsertion(HugeGraph graph) {
        super(GraphDatabaseType.HUGEGRAPH_CORE, null);
        this.graph = graph;
        this.vl = this.graph.vertexLabel(HugeGraphDatabase.NODE);
        this.reset();
    }

    private void reset() {
        this.vertices = new ArrayList<>();
        this.edges = new ArrayList<>(EDGE_BATCH_NUMBER);
    }

    @Override
    protected Integer getOrCreate(String value) {
        Integer v = Integer.valueOf(value);

        if (!this.allVertices.contains(v)) {
            this.allVertices.add(v);
            this.vertices.add(v);
        }
        return v;
    }

    @Override
    protected void relateNodes(Integer src, Integer dest) {
        this.edges.add(Pair.of(src, dest));
        if (this.edges.size() >= EDGE_BATCH_NUMBER) {
            this.batchCommit();
            this.reset();
        }
    }

    private void batchCommit() {
        List<Integer> vertices = this.vertices;
        List<Pair<Integer, Integer>> edges = this.edges;

        this.pool.submit(() -> {
            for (Integer v : vertices) {
                this.graph.addVertex(T.id, v, T.label, HugeGraphDatabase.NODE);
            }
            HugeVertex source;
            HugeVertex target;
            for (Pair<Integer, Integer> e: edges) {
                source = new HugeVertex(this.graph,
                                        IdGenerator.of(e.getLeft()), this.vl);
                target = new HugeVertex(this.graph,
                                        IdGenerator.of(e.getRight()), this.vl);
                source.addEdge(HugeGraphDatabase.SIMILAR, target);
            }
            this.graph.tx().commit();
        });
    }

    @Override
    protected void post() {
        this.pool.shutdown();
        try {
            this.pool.awaitTermination(60 * 5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
