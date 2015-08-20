package eu.socialsensor.insert;

import com.sparsity.sparksee.gdb.Graph;
import com.sparsity.sparksee.gdb.Session;
import com.sparsity.sparksee.gdb.Value;

import eu.socialsensor.graphdatabases.SparkseeGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;

public class SparkseeMassiveInsertion extends InsertionBase<Long> implements Insertion
{
    private final Session session;
    private final Graph sparkseeGraph;
    private int operations;

    public SparkseeMassiveInsertion(Session session)
    {
        super(GraphDatabaseType.SPARKSEE, null /* resultsPath */);
        this.session = session;
        this.sparkseeGraph = session.getGraph();
        this.operations = 0;
    }

    @Override
    public Long getOrCreate(String value)
    {
        Value sparkseeValue = new Value();
        return sparkseeGraph.findOrCreateObject(SparkseeGraphDatabase.NODE_ATTRIBUTE, sparkseeValue.setString(value));
    }

    @Override
    public void relateNodes(Long src, Long dest)
    {
        sparkseeGraph.newEdge(SparkseeGraphDatabase.EDGE_TYPE, src, dest);
        operations++;
        if (operations == 10000)
        {
            session.commit();
            session.begin();
            operations = 0;
        }
    }
}
