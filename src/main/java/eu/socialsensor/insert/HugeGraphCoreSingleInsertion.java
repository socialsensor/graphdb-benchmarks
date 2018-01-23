package eu.socialsensor.insert;

import java.io.File;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.structure.constant.T;
import eu.socialsensor.graphdatabases.HugeGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.HugeGraphUtils;

public class HugeGraphCoreSingleInsertion extends InsertionBase<Vertex> {

    private final HugeGraph graph;

    public HugeGraphCoreSingleInsertion(HugeGraph graph, File resultPath) {
        super(GraphDatabaseType.HUGEGRAPH_CORE, resultPath);
        this.graph = graph;
    }

    @Override
    protected Vertex getOrCreate(String value) {
        Vertex vertex = null;
        if (!HugeGraphUtils.isStringEmpty(value)) {
            Integer id = Integer.valueOf(value);
            vertex = this.graph.vertices(id).next();
            if (vertex == null) {
                vertex = this.graph.addVertex(T.label, HugeGraphDatabase.NODE,
                                              HugeGraphDatabase.NODE_ID, id);
                this.graph.tx().commit();
            }
        }
        return vertex;
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest) {
        src.addEdge(HugeGraphDatabase.SIMILAR, dest);
        this.graph.tx().commit();
    }
}
