package eu.socialsensor.insert;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.sparsity.sparksee.gdb.Graph;
import com.sparsity.sparksee.gdb.Session;
import com.sparsity.sparksee.gdb.Value;

public class SparkseeMassiveInsertion implements Insertion {
	
	private Logger logger = Logger.getLogger(SparkseeMassiveInsertion.class);
	
	Session session;
	Graph sparkseeGraph;
	
	public SparkseeMassiveInsertion(Session session) {
		this.session = session;
		this.sparkseeGraph = session.getGraph();
	}
	
	@Override
	public void createGraph(String datasetDir) {
		logger.setLevel(Level.INFO);
		logger.info("Loading data in massive mode in Sparksee database . . . .");
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
			String line;
			int lineCounter = 1;
			long srcNode, dstNode;
			
			int nodeType = sparkseeGraph.findType("node");
			int nodeAttribute = sparkseeGraph.findAttribute(nodeType, "nodeId");
			int edgeType = sparkseeGraph.findType("similar");
			
			Value value = new Value();
			int operations = 0;
			session.begin();
			while((line = reader.readLine()) != null) {
				if(lineCounter > 4) {
					String[] parts = line.split("\t");
					
					srcNode = sparkseeGraph.findOrCreateObject(nodeAttribute, value.setString(parts[0]));
					dstNode = sparkseeGraph.findOrCreateObject(nodeAttribute, value.setString(parts[1]));
					
					sparkseeGraph.newEdge(edgeType, srcNode, dstNode);
					
					operations++;
					if(operations == 10000) {
						operations = 0;
						session.commit();
						session.begin();
					}
				}
				lineCounter++;
			}
			session.commit();
			reader.close();
		}
		catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

}
