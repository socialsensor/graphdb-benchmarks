package eu.socialsensor.query;

/**
 * Represents the queries for each graph database
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 * 
 */
public interface Query {
	
	/**
	 * Iterates over the nodes and finds the neighbours
	 * of each node
	 */
	public void findNeighborsOfAllNodes();
	
	/**
	 * Iterates over the edges and finds the adjacent
	 * nodes of each edge
	 */
	public void findNodesOfAllEdges();
	
	/**
	 * Finds the shortest path between the first node
	 * and 100 randomly picked nodes
	 */
	public void findShortestPaths();
	
}
