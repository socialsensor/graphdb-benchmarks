package eu.socialsensor.insert;

import com.thinkaurelius.titan.core.TitanGraph;

import eu.socialsensor.main.GraphDatabaseType;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.File;

/**
 * Implementation of massive Insertion in Titan graph database.
 * April 2016(lindsaysmith): Massive Insertion into Titan 1.0.0 doesn't have specialized classes, 
 * just different configuration recommendations.  This insertion is currently implemented the same 
 * way as the simple insertion.
 * 
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * @author Lindsay Smith lindsaysmith@google.com
 */
public class TitanMassiveInsertion extends InsertionBase<Vertex>
{
    private final TitanSingleInsertion delegate;
    
    public TitanMassiveInsertion(TitanGraph titanGraph, GraphDatabaseType type, File resultsPath) {
      super(type, resultsPath);
      delegate = new TitanSingleInsertion(titanGraph, type, resultsPath);
    }    

    @Override
    public Vertex getOrCreate(String value)
    {
        return delegate.getOrCreate(value);
    }

    @Override
    public void relateNodes(Vertex src, Vertex dest)
    {
        delegate.relateNodes(src, dest);
    }
}
