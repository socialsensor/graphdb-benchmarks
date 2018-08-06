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

import org.apache.tinkerpop.gremlin.structure.T;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;

import eu.socialsensor.graphdatabases.HugeGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;

public class HugeGraphCoreMassiveInsertion extends InsertionBase<Integer> {

    private ExecutorService pool = Executors.newFixedThreadPool(8);
    private static final int EDGE_BATCH_NUMBER = 500;

    private Set<Integer> allVertices = new HashSet<>();
    private Set<Integer> vertices = new HashSet<>();

    private List<Integer> source = new ArrayList<>(EDGE_BATCH_NUMBER);
    private List<Integer> target = new ArrayList<>(EDGE_BATCH_NUMBER);

    private static int edgeNumber = 0;

    private HugeGraph graph = null;
    private VertexLabel vl = null;

    public HugeGraphCoreMassiveInsertion(HugeGraph graph) {
        super(GraphDatabaseType.HUGEGRAPH_CORE, null);
        this.graph = graph;
        this.vl = this.graph.vertexLabel(HugeGraphDatabase.NODE);
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
        this.source.add(src);
        this.target.add(dest);
        if (++edgeNumber == EDGE_BATCH_NUMBER) {
            batchCommit();
            edgeNumber = 0;
        }
    }

    private void batchCommit() {
        Set<Integer> vertices = this.vertices;
        this.vertices = new HashSet<>();
        List<Integer> src = this.source;
        this.source = new ArrayList<>(500);
        List<Integer> tgt = this.target;
        this.target = new ArrayList<>(500);

        this.pool.submit(() -> {
            for (Integer n : vertices) {
                this.graph.addVertex(T.id, n, T.label, HugeGraphDatabase.NODE);
            }
            for (int i = 0; i < 500; i++) {
                HugeVertex source = new HugeVertex(this.graph,
                                                   IdGenerator.of(src.get(i)),
                                                   this.vl);
                HugeVertex target = new HugeVertex(this.graph,
                                                   IdGenerator.of(tgt.get(i)),
                                                   this.vl);
                source.addEdge(HugeGraphDatabase.SIMILAR, target);
            }
            this.graph.tx().commit();
        });
    }

    @Override
    protected void post() {
        this.graph.tx().commit();
    }
}
