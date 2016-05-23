package eu.socialsensor.insert;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.attribute.Cmp;

import eu.socialsensor.main.GraphDatabaseType;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.File;
import java.util.Iterator;

/**
 * Implementation of single Insertion in Titan graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * @author Lindsay Smith lindsaysmith@google.com
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
        Integer intValue = Integer.valueOf(value.trim());
        final Vertex v;
        Iterator<TitanVertex> i = titanGraph.query().has("nodeId", Cmp.EQUAL, intValue).vertices().iterator();
        if (i.hasNext())
        {
            v = (Vertex) i.next();
        }
        else
        {            
            v = titanGraph.addVertex();
            v.property("nodeId", intValue);
            titanGraph.tx().commit();
        }
        return v;
    }

    @Override
    public void relateNodes(Vertex src, Vertex dest)
    {        
        try
        {            
            src.addEdge("similar", dest);
            titanGraph.tx().commit();
        }
        catch (Exception e)
        {
            titanGraph.tx().rollback(); //TODO(amcp) why can this happen? doesn't this indicate illegal state?
        }
    }
}
