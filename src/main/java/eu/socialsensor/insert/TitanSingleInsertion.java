package eu.socialsensor.insert;

import java.io.File;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanId;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;

import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of single Insertion in Titan graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public class TitanSingleInsertion extends InsertionBase<Vertex>
{
    private final TitanGraph titanGraph;

    public TitanSingleInsertion(TitanGraph titanGraph, GraphDatabaseType type, File resultsPath)
    {
        super(type, resultsPath);
        this.titanGraph = titanGraph;
    }

    @Override
    public Vertex getOrCreate(String value)
    {
        Integer intValue = Integer.valueOf(value);
        final Vertex v;
        if (titanGraph.query().has("nodeId", Compare.EQUAL, intValue).vertices().iterator().hasNext())
        {
            v = (Vertex) titanGraph.query().has("nodeId", Compare.EQUAL, intValue).vertices().iterator().next();
        }
        else
        {
            final long titanVertexId = TitanId.toVertexId(intValue);
            v = titanGraph.addVertex(titanVertexId);
            v.setProperty("nodeId", intValue);
            titanGraph.commit();
        }
        return v;
    }

    @Override
    public void relateNodes(Vertex src, Vertex dest)
    {
        try
        {
            titanGraph.addEdge(null, src, dest, "similar");
            titanGraph.commit();
        }
        catch (Exception e)
        {
            titanGraph.rollback(); //TODO(amcp) why can this happen? doesn't this indicate illegal state?
        }
    }
}
