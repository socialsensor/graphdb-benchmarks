package eu.socialsensor.insert;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.graph.batch.OGraphBatchInsertBasic;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of massive Insertion in OrientDB graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public class OrientMassiveInsertion extends InsertionBase<Long> implements Insertion
{
    private static final int ESTIMATED_ENTRIES = 1000000;
    private static final int AVERAGE_NUMBER_OF_EDGES_PER_NODE = 40;
    private static final int NUMBER_OF_ORIENT_CLUSTERS = 16;
    private final OGraphBatchInsertBasic graph;

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

        graph = new OGraphBatchInsertBasic(url);
        graph.setAverageEdgeNumberPerNode(AVERAGE_NUMBER_OF_EDGES_PER_NODE);
        graph.setEstimatedEntries(ESTIMATED_ENTRIES);
        graph.setIdPropertyName("nodeId");
        graph.begin();
    }

    @Override
    protected void post() {
        graph.end();
    }

    @Override
    protected Long getOrCreate(String value)
    {
        final long v = Long.parseLong(value);
        graph.createVertex(v);
        return v;
    }

    @Override
    protected void relateNodes(Long src, Long dest)
    {
        graph.createEdge(src, dest);
    }
}
