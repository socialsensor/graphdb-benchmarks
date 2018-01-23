package eu.socialsensor.insert;

import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;
import eu.socialsensor.graphdatabases.HugeGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.HugeGraphUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;

public class HugeGraphSingleInsertion extends InsertionBase<Vertex> {

    private static final Logger LOG = LogManager.getLogger();
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
            vertex = graphManager.getVertex(id);
            if (vertex == null) {
                vertex = new Vertex(HugeGraphDatabase.NODE)
                        .property(HugeGraphDatabase.NODE_ID, id);
                graphManager.addVertex(T.label, HugeGraphDatabase.NODE,
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
        graphManager.addEdge(src.id(), HugeGraphDatabase.SIMILAR, dest.id());
    }
}
