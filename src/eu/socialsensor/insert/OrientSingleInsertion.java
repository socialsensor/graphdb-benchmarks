package eu.socialsensor.insert;

import com.orientechnologies.orient.core.index.OIndex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import eu.socialsensor.benchmarks.SingleInsertionBenchmark;
import eu.socialsensor.utils.Utils;
import org.apache.log4j.Level;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of single Insertion in OrientDB graph database
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 * 
 */
public class OrientSingleInsertion extends OrientAbstractInsertion {

	public OrientSingleInsertion(OrientBaseGraph orientGraph, OIndex vertices) {
		super(orientGraph, vertices);
	}

	@Override
	public void createGraph(String datasetDir) {
		INSERTION_TIMES_OUTPUT_PATH = SingleInsertionBenchmark.INSERTION_TIMES_OUTPUT_PATH + ".orient";
		logger.setLevel(Level.INFO);
		count++;
		logger.info("Incrementally loading data in Orient database . . . .");
		List<Double> insertionTimes = new ArrayList<Double>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
			String line;
			int lineCounter = 1;
			int nodesCounter = 0;
			OrientVertex srcVertex, dstVertex;
			Iterable<OrientVertex> cache;

			long start = System.currentTimeMillis();
			long duration;
			while ((line = reader.readLine()) != null) {
				if (lineCounter > 4) {
					String[] parts = line.split("\t");

					final Integer key = Integer.parseInt(parts[0]);
					cache = vertexIndexLookup(key);
					if (cache.iterator().hasNext()) {
						srcVertex = cache.iterator().next();
					} 
					else {
						srcVertex = orientGraph.addVertex(null, "nodeId", key);
						vertices.put(key, srcVertex);
						orientGraph.commit();
						nodesCounter++;
					}

					if (nodesCounter == 1000) {
						duration = System.currentTimeMillis() - start;
						insertionTimes.add((double) duration);
						nodesCounter = 0;
						start = System.currentTimeMillis();
					}

					final Integer key2 = Integer.parseInt(parts[0]);

					cache = vertexIndexLookup(key2);
					if (cache.iterator().hasNext()) {
						dstVertex = cache.iterator().next();
					} 
					else {
						dstVertex = orientGraph.addVertex(null, "nodeId", key2);
						vertices.put(key2, dstVertex);
						orientGraph.commit();
						nodesCounter++;
					}

					orientGraph.addEdge(null, srcVertex, dstVertex, "similar");
					orientGraph.commit();

					if (nodesCounter == 1000) {
						duration = System.currentTimeMillis() - start;
						insertionTimes.add((double) duration);
						nodesCounter = 0;
						start = System.currentTimeMillis();
					}
				}
				lineCounter++;
			}

			duration = System.currentTimeMillis() - start;
			insertionTimes.add((double) duration);
			reader.close();
		} 
		catch (IOException ioe) {
			System.out.println(ioe);
			ioe.printStackTrace();
		} 
		catch (Exception e) {
			System.out.println(e);
			orientGraph.rollback();
		}
		Utils utils = new Utils();
		utils.writeTimes(insertionTimes, OrientSingleInsertion.INSERTION_TIMES_OUTPUT_PATH + "." + count);
	}
}
