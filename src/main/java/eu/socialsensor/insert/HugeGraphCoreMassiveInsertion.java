/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package eu.socialsensor.insert;

import java.util.HashSet;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.T;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;
import eu.socialsensor.graphdatabases.HugeGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;

public class HugeGraphCoreMassiveInsertion extends InsertionBase<Integer> {

    private Set<Integer> vertices = new HashSet<>();

    private static final int VERTEX_BATCH_NUMBER = 500;
    private static final int EDGE_BATCH_NUMBER = 500;

    private static int edgeNumber = 0;
    private static int vertexNumber = 0;

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
        this.graph.addVertex(T.id, v, T.label, HugeGraphDatabase.NODE);
        if (!this.vertices.contains(v)) {
            this.vertices.add(v);
            vertexNumber++;
        }

        if (vertexNumber == VERTEX_BATCH_NUMBER) {
            this.graph.tx().commit();
            vertexNumber = 0;
        }
        return v;
    }

    @Override
    protected void relateNodes(Integer src, Integer dest) {
        HugeVertex source = new HugeVertex(graph, IdGenerator.of(src), vl);
        HugeVertex target = new HugeVertex(graph, IdGenerator.of(dest), vl);
        source.addEdge(HugeGraphDatabase.SIMILAR, target);
        edgeNumber++;
        if (edgeNumber == EDGE_BATCH_NUMBER) {
            graph.tx().commit();
            edgeNumber = 0;
        }
    }

    @Override
    protected void post() {
        this.graph.tx().commit();
    }
}
