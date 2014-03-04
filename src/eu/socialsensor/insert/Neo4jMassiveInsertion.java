package eu.socialsensor.insert;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;


public class Neo4jMassiveInsertion implements Insertion {
	
	private BatchInserter inserter = null;
//	private BatchInserterIndexProvider indexProvider = null;
	private BatchInserterIndex nodes = null;
	Map<Long, Long> cache = new HashMap<Long, Long>();
	
	private static enum RelTypes implements RelationshipType {
	    SIMILAR
	}
	
	
	public Neo4jMassiveInsertion(BatchInserter inserter, BatchInserterIndex index) {
		this.inserter = inserter;
		this.nodes = index;
	}
	
	public void createGraph(String datasetDir) {
		System.out.println("Loading data in massive mode in Neo4j database . . . .");
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
			String line;
			int lineCounter = 1;
//			Map<String, Object> properties;
//			IndexHits<Long> cache;
			long srcNode, dstNode;
			while((line = reader.readLine()) != null) {
				if(lineCounter > 4) {
					String[] parts = line.split("\t");
					
					srcNode = getOrCreate(parts[0]);
					dstNode = getOrCreate(parts[1]);
					
					inserter.createRelationship(srcNode, dstNode, RelTypes.SIMILAR, null);
				}
				lineCounter++;
			}
			reader.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		nodes.flush();
	}
	
	private long getOrCreate(String value) {
		Long id = cache.get(Long.valueOf(value));
		if(id == null) {
			Map<String, Object> properties = MapUtil.map("nodeId", value);
			id = inserter.createNode(properties);
			cache.put(Long.valueOf(value), id);
			nodes.add(id, properties);
		}
		return id;
	}



}
