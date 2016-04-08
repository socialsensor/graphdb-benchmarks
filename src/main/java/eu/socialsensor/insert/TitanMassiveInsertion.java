package eu.socialsensor.insert;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.core.TitanVertex;
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
public abstract class TitanMassiveInsertion extends InsertionBase<Vertex>
{
    private static final Logger logger = LogManager.getLogger();
    protected final StandardTitanGraph graph;
    protected final StandardTitanTx tx;

    Map<Long, TitanVertex> vertexCache;

    public TitanMassiveInsertion(StandardTitanGraph graph, GraphDatabaseType type)
    {
        super(type, null /* resultsPath */); // no temp files for massive load
                                             // insert
        this.graph = graph;
        Preconditions.checkArgument(graph.getOpenTransactions().isEmpty(),
                "graph may not have open transactions at this point");
        graph.tx().open();
        this.tx = (StandardTitanTx) Iterables.getOnlyElement(graph.getOpenTransactions());
        this.vertexCache = Maps.newHashMap();
    }

    @Override
    public void relateNodes(Vertex src, Vertex dest)
    {
        src.addEdge(SIMILAR, dest);
    }

    @Override
    protected void post() {
        logger.trace("vertices: " + vertexCache.size());
        tx.commit(); //mutation work is done here
        Preconditions.checkState(graph.getOpenTransactions().isEmpty());
    }

    public static final TitanMassiveInsertion create(StandardTitanGraph graph, GraphDatabaseType type,
        boolean customIds) {
        if(customIds) {
            return new TitanMassiveCustomIds(graph, type);
        } else {
            return new TitanMassiveDefaultIds(graph, type);
        }
    }
}
