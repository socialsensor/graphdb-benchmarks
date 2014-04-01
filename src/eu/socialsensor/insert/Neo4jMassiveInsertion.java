package eu.socialsensor.insert;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;

import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;

/**
 * Implementation of massive Insertion in Neo4j
 * graph database
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 * 
 */
public class Neo4jMassiveInsertion implements Insertion {
	
	private BatchInserter inserter = null;
	private BatchInserterIndex nodes = null;
	Map<Long, Long> cache = new HashMap<Long, Long>();
	
	public Neo4jMassiveInsertion(BatchInserter inserter, BatchInserterIndex index) {
		this.inserter = inserter;
		this.nodes = index;
	}
	
	@Override
	public void createGraph(String datasetDir) {
		System.out.println("Loading data in massive mode in Neo4j database . . . .");
		inserter.createDeferredSchemaIndex(Neo4jGraphDatabase.NODE_LABEL).on("nodeId").create();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
			String line;
			int lineCounter = 1;
			long srcNode, dstNode;
			while((line = reader.readLine()) != null) {
				if(lineCounter > 4) {
					String[] parts = line.split("\t");
					
					srcNode = getOrCreate(parts[0]);
					dstNode = getOrCreate(parts[1]);
					
					inserter.createRelationship(srcNode, dstNode, Neo4jGraphDatabase.RelTypes.SIMILAR, null);
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
			id = inserter.createNode(properties, Neo4jGraphDatabase.NODE_LABEL);
			cache.put(Long.valueOf(value), id);
			nodes.add(id, properties);
		}
		return id;
	}



}
