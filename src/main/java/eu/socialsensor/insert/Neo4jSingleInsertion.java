package eu.socialsensor.insert;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.main.BenchmarkingException;
import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of single Insertion in Neo4j graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public class Neo4jSingleInsertion extends InsertionBase<Node>
{
    private final GraphDatabaseService neo4jGraph;

    public Neo4jSingleInsertion(GraphDatabaseService neo4jGraph, File resultsPath)
    {
        super(GraphDatabaseType.NEO4J, resultsPath);
        this.neo4jGraph = neo4jGraph;
    }

    public Node getOrCreate(String nodeId)
    {
        Node result = null;

        try(final Transaction tx = neo4jGraph.beginTx())
        {
            try
            {
                String queryString = "MERGE (n:Node {nodeId: {nodeId}}) RETURN n";
                Map<String, Object> parameters = new HashMap<String, Object>();
                parameters.put("nodeId", nodeId);
                ResourceIterator<Node> resultIterator = neo4jGraph.execute(queryString, parameters).columnAs("n");
                result = resultIterator.next();
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get or create node " + nodeId, e);
            }
        }

        return result;
    }

    @Override
    public void relateNodes(Node src, Node dest)
    {
        try (final Transaction tx = neo4jGraph.beginTx())
        {
            try
            {
                src.createRelationshipTo(dest, Neo4jGraphDatabase.RelTypes.SIMILAR);
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to relate nodes", e);
            }
        }
    }
}
