package eu.socialsensor.main;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Enum containing constants that correspond to each database.
 * 
 * @author Alexander Patrikalakis
 */
public enum GraphDatabaseType
{
    TITAN_BERKELEYDB("Titan", "berkeleyje", "tbdb"),
    TITAN_DYNAMODB("Titan", "com.amazon.titan.diskstorage.dynamodb.DynamoDBStoreManager", "tddb"),
    TITAN_CASSANDRA("Titan", "cassandra", "tc"),
    TITAN_CASSANDRA_EMBEDDED("TitanEmbedded", "embeddedcassandra", "tce"),
    TITAN_HBASE("Titan", "hbase", "thb"),
    TITAN_CLOUDBIGTABLE("Titan", "hbase", "tcbt"),
    TITAN_PERSISTIT("TitanEmbedded", "persistit", "tp"),    
    ORIENT_DB("OrientDB", null, "orient"),
    NEO4J("Neo4j", null, "neo4j"),
    SPARKSEE("Sparksee", null, "sparksee");

    private final String backend;
    private final String api;
    private final String shortname;

    public static final Map<String, GraphDatabaseType> STRING_REP_MAP = new HashMap<String, GraphDatabaseType>();
    public static final Set<GraphDatabaseType> TITAN_FLAVORS = new HashSet<GraphDatabaseType>();
    static
    {
        for (GraphDatabaseType db : values())
        {
            STRING_REP_MAP.put(db.getShortname(), db);
        }
        TITAN_FLAVORS.add(TITAN_BERKELEYDB);
        TITAN_FLAVORS.add(TITAN_DYNAMODB);
        TITAN_FLAVORS.add(TITAN_CASSANDRA);
        TITAN_FLAVORS.add(TITAN_CASSANDRA_EMBEDDED);
        TITAN_FLAVORS.add(TITAN_HBASE);
        TITAN_FLAVORS.add(TITAN_PERSISTIT);
        TITAN_FLAVORS.add(TITAN_CLOUDBIGTABLE);
    }

    private GraphDatabaseType(String api, String backend, String shortname)
    {
        this.api = api;
        this.backend = backend;
        this.shortname = shortname;
    }

    public String getBackend()
    {
        return backend;
    }

    public String getApi()
    {
        return api;
    }

    public String getShortname()
    {
        return shortname;
    }
}
