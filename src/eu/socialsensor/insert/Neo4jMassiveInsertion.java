package eu.socialsensor.insert;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;


public class Neo4jMassiveInsertion implements Insertion {
	
	private BatchInserter inserter = null;
//	private BatchInserterIndexProvider indexProvider = null;
//	private BatchInserterIndex nodes = null;
	Map<Long, Long> cache = new HashMap<Long, Long>();
	
	private static enum RelTypes implements RelationshipType {
	    SIMILAR
	}
	
	public static void main(String args[]) {
		Neo4jMassiveInsertion test = new Neo4jMassiveInsertion();
		test.startup("data/neo4j");
		test.createGraph("data/amazonEdges.txt");
		test.shutdown();
	}
	
	/**
	 * Start neo4j database and configure for massive insertion
	 * @param neo4jDBDir
	 */
	public void startup(String neo4jDBDir) {
		System.out.println("The Neo4j database is now starting . . . .");
		Map<String, String> config = new HashMap<String, String>();
		config.put("cache_type", "none");
		config.put("use_memory_mapped_buffers", "true");
		config.put("neostore.nodestore.db.mapped_memory", "200M");
		config.put("neostore.relationshipstore.db.mapped_memory", "1000M");
		config.put("neostore.propertystore.db.mapped_memory", "250M");
		config.put("neostore.propertystore.db.strings.mapped_memory", "250M");
		inserter = BatchInserters.inserter(neo4jDBDir, config);
//		indexProvider = new LuceneBatchInserterIndexProvider(inserter);
//		nodes = indexProvider.nodeIndex("nodes", MapUtil.stringMap("type", "exact"));
	}
	
	public void shutdown() {
		System.out.println("The Neo4j database is now shuting down . . . .");
		if(inserter != null) {
//			indexProvider.shutdown();
			inserter.shutdown();
//			indexProvider = null;
			inserter = null;
		}
	}
	
	public void createGraph(String datasetDir) {
		System.out.println("Creating the Neo4j database . . . .");
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
	}
	
	private long getOrCreate(String value) {
		Long id = cache.get(Long.valueOf(value));
		if(id == null) {
			Map<String, Object> properties = MapUtil.map("nodeId", value);
			id = inserter.createNode(properties);
			cache.put(Long.valueOf(value), id);
		}
		return id;
	}



}
