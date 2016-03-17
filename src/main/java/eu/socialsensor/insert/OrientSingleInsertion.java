package eu.socialsensor.insert;

import java.io.File;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of single Insertion in OrientDB graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public final class OrientSingleInsertion extends InsertionBase<Vertex>
{
    protected final OrientGraph orientGraph;
    protected final OIndex<?> index;

    public OrientSingleInsertion(OrientGraph orientGraph, File resultsPath)
    {
        super(GraphDatabaseType.ORIENT_DB, resultsPath);
        this.orientGraph = orientGraph;
        this.index = this.orientGraph.getRawGraph().getMetadata().getIndexManager().getIndex("V.nodeId");
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest)
    {
        orientGraph.addEdge(null, src, dest, "similar");

        // TODO why commit twice? is this a nested transaction?
        if (orientGraph instanceof TransactionalGraph)
        {
            orientGraph.commit();
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
