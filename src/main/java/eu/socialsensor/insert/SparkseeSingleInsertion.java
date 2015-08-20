package eu.socialsensor.insert;

import java.io.File;

import com.sparsity.sparksee.gdb.Graph;
import com.sparsity.sparksee.gdb.Session;
import com.sparsity.sparksee.gdb.Value;

import eu.socialsensor.graphdatabases.SparkseeGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;

public class SparkseeSingleInsertion extends InsertionBase<Long>
{
    private final Session session;
    private final Graph sparkseeGraph;

    Value value = new Value();

    public SparkseeSingleInsertion(Session session, File resultsPath)
    {
        // no temp files for massive load insert
        super(GraphDatabaseType.SPARKSEE, resultsPath);
        this.session = session;
        this.sparkseeGraph = session.getGraph();
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
        session.begin();
        sparkseeGraph.newEdge(SparkseeGraphDatabase.EDGE_TYPE, src, dest);
        session.commit();
    }

}
