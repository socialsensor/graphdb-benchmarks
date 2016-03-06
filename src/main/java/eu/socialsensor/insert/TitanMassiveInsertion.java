package eu.socialsensor.insert;

import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.util.TitanId;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;

import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of massive Insertion in Titan graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public class TitanMassiveInsertion extends InsertionBase<Vertex>
{
    private final StandardTitanGraph graph;
    private final StandardTitanTx tx;
    private final VertexLabel nodeLabel;
    private Map<Long, TitanVertex> vertexCache;

    public TitanMassiveInsertion(StandardTitanGraph graph, GraphDatabaseType type)
    {
        super(type, null /* resultsPath */); // no temp files for massive load
                                             // insert
        this.graph = graph;
        Preconditions.checkArgument(graph.getOpenTransactions().isEmpty(),
                "graph may not have open transactions at this point");
        graph.tx().open();
        this.tx = (StandardTitanTx) Iterables.getOnlyElement(graph.getOpenTransactions());
        this.nodeLabel = tx.getVertexLabel(NODE_LABEL);
        this.vertexCache = Maps.newHashMap();
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

        synchronized(vertexCache) {
            //add to cache for first time
            if(!vertexCache.containsKey(longPositiveVal)) {
                final TitanVertex vertex = tx.addVertex(titanVertexId, nodeLabel /*vertexLabel*/);
                vertex.property(NODEID, longVal);
                vertexCache.put(longPositiveVal, vertex);
            }
        }
        return vertexCache.get(longPositiveVal);
    }

    @Override
    public void relateNodes(Vertex src, Vertex dest)
    {
        src.addEdge(SIMILAR, dest);
    }

    @Override
    protected void post() {
        tx.commit(); //mutation work is done here
        Preconditions.checkState(graph.getOpenTransactions().isEmpty());
    }
}
