package eu.socialsensor.insert;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;

import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of massive Insertion in Neo4j graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public final class Neo4jMassiveInsertion extends InsertionBase<Long>
{
    private final BatchInserter inserter;
    Map<Long, Long> cache = new HashMap<Long, Long>();

    public Neo4jMassiveInsertion(BatchInserter inserter)
    {
        super(GraphDatabaseType.NEO4J, null /* resultsPath */);
        this.inserter = inserter;
    }

    @Override
    protected Long getOrCreate(String value)
    {
        Long id = cache.get(Long.valueOf(value));
        if (id == null)
        {
            Map<String, Object> properties = MapUtil.map("nodeId", value);
            id = inserter.createNode(properties, Neo4jGraphDatabase.NODE_LABEL);
            cache.put(Long.valueOf(value), id);
        }
        return id;
    }

    @Override
    protected void relateNodes(Long src, Long dest)
    {
        inserter.createRelationship(src, dest, Neo4jGraphDatabase.RelTypes.SIMILAR, null);
    }
}
