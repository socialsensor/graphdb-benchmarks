package eu.socialsensor.insert;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

import eu.socialsensor.main.GraphDatabaseType;

/**
 * A Titan insertion strategy for using Titan-generated vertex ids.
 * @author Alexander Patrikalakis
 *
 */
public class TitanMassiveDefaultIds extends TitanMassiveInsertion {
    public TitanMassiveDefaultIds(StandardTitanGraph graph, GraphDatabaseType type) {
        super(graph, type);
    }
    
    @Override
    public Vertex getOrCreate(String value)
    {
        //the value used in data files
        final Long longVal = Long.valueOf(value); 

        //add to cache for first time
        if(!vertexCache.containsKey(longVal)) {
            vertexCache.put(longVal, tx.addVertex(T.label, TitanMassiveInsertion.NODE_LABEL, NODEID, longVal));
        }
        return vertexCache.get(longVal);
    }
}