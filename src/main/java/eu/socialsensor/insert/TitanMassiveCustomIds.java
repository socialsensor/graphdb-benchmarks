package eu.socialsensor.insert;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.util.TitanId;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

import eu.socialsensor.main.GraphDatabaseType;

/**
 * A Titan massive insertion strategy that uses custom vertex ids.
 * @author Alexander Patrikalakis
 *
 */
public class TitanMassiveCustomIds extends TitanMassiveInsertion {
    private final VertexLabel nodeLabel;
    public TitanMassiveCustomIds(StandardTitanGraph graph, GraphDatabaseType type) {
        super(graph, type);
        this.nodeLabel = tx.getVertexLabel(NODE_LABEL);
    }
    
    @Override
    public Vertex getOrCreate(String value)
    {
        final Long longVal = Long.valueOf(value); //the value used in data files
        //the value used in data files sometimes is zero so add one for the purposes of generating ids
        final Long longPositiveVal = Long.valueOf(value) + 1;
        //send everything to partition 1 by adding 1
        final long titanVertexId =
                TitanId.toVertexId((longPositiveVal << 1) + 1 /*move over 1 bit for 2 partitions (2^1 = 2)*/);
        // TODO(amcp) maybe this is slow and looking up by titanVertexId results in nothing getting committed.
        // instead maintain my own index
        // final GraphTraversal<Vertex, Vertex> t = tx.traversal().V().has(NODEID, longVal);

        //add to cache for first time
        if(!vertexCache.containsKey(longPositiveVal)) {
            final TitanVertex vertex = tx.addVertex(titanVertexId, nodeLabel /*vertexLabel*/);
            vertex.property(NODEID, longVal);
            vertexCache.put(longPositiveVal, vertex);
        }
        return vertexCache.get(longPositiveVal);
    }
}