package eu.socialsensor.insert;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.graph.batch.OGraphBatchInsertBasic;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of massive Insertion in OrientDB graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public class OrientMassiveInsertion extends InsertionBase<Vertex> implements Insertion
{
    private static final int ESTIMATED_ENTRIES = 1000000;
    private static final int AVERAGE_NUMBER_OF_EDGES_PER_NODE = 40;
    private static final int NUMBER_OF_ORIENT_CLUSTERS = 16;
    private final OGraphBatchInsertBasic graph;

    protected final OrientGraphNoTx orientGraph;
    protected final OIndex<?> index;

    public OrientMassiveInsertion(final String url)
    {
        super(GraphDatabaseType.ORIENT_DB, null /* resultsPath */);
        OGlobalConfiguration.ENVIRONMENT_CONCURRENT.setValue(false);
        OrientGraphNoTx transactionlessGraph = new OrientGraphNoTx(url);
        for (int i = 0; i < NUMBER_OF_ORIENT_CLUSTERS; ++i)
        {
            transactionlessGraph.getVertexBaseType().addCluster("v_" + i);
            transactionlessGraph.getEdgeBaseType().addCluster("e_" + i);
        }
        transactionlessGraph.shutdown();

        orientGraph = new OrientGraphNoTx(url);
        this.index = this.orientGraph.getRawGraph().getMetadata().getIndexManager().getIndex("V.nodeId");

        graph = new OGraphBatchInsertBasic(url);
        graph.setAverageEdgeNumberPerNode(AVERAGE_NUMBER_OF_EDGES_PER_NODE);
        graph.setEstimatedEntries(ESTIMATED_ENTRIES);
        graph.setIdPropertyName("nodeId");
        graph.setEdgeClass("similar");
        graph.begin();
    }

//    @Override
//    protected void post() {
//        graph.end();
//    }

//    @Override
//    protected Long getOrCreate(String value)
//    {
//        final long v = Long.parseLong(value);
//        graph.createVertex(v);
//        return v;
//    }

//    @Override
//    protected void relateNodes(Long src, Long dest)
//    {
//        graph.createEdge(src, dest);
//    }



    @Override
    protected void relateNodes(Vertex src, Vertex dest)
    {
        orientGraph.addEdge(null, src, dest, "similar");

        // TODO why commit twice? is this a nested transaction?
        if (orientGraph instanceof TransactionalGraph)
        {
            orientGraph.commit();
        }
    }

    @Override
    protected Vertex getOrCreate(final String value)
    {
        final int key = Integer.parseInt(value);

        Vertex v;
        final OIdentifiable rec = (OIdentifiable) index.get(key);
        if (rec != null)
        {
            return orientGraph.getVertex(rec);
        }

        v = orientGraph.addVertex(key, "nodeId", key);


        if (orientGraph instanceof TransactionalGraph)
        {
            orientGraph.commit();
        }

        return v;
    }

    @Override
    protected void post()
    {
        super.post();
        if (orientGraph instanceof TransactionalGraph)
        {
            orientGraph.commit();
        }
    }
}
