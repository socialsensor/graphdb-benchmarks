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

import java.io.File;

import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;

import eu.socialsensor.graphdatabases.HugeGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.HugeGraphUtils;

public class HugeGraphSingleInsertion extends InsertionBase<Vertex> {

    private final GraphManager graphManager;

    public HugeGraphSingleInsertion(GraphManager graphManager,
                                    File resultPath) {
        super(GraphDatabaseType.HUGEGRAPH, resultPath);
        this.graphManager = graphManager;
    }

    @Override
    protected Vertex getOrCreate(String value) {
        Vertex vertex = null;
        if (!HugeGraphUtils.isStringEmpty(value)) {
            String id = HugeGraphUtils.createId(HugeGraphDatabase.NODE, value);
            vertex = this.graphManager.getVertex(id);
            if (vertex == null) {
                vertex = new Vertex(HugeGraphDatabase.NODE);
                vertex.property(HugeGraphDatabase.NODE_ID, id);
                this.graphManager.addVertex(T.label, HugeGraphDatabase.NODE,
                                            HugeGraphDatabase.NODE_ID, id);
            }
        }
        return vertex;
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest) {
        Edge edge = new Edge(HugeGraphDatabase.SIMILAR);
        edge.source(src);
        edge.target(dest);
        this.graphManager.addEdge(src.id(), HugeGraphDatabase.SIMILAR, dest.id());
    }
}
