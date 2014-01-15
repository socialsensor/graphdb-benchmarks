package insert;

public interface Insertion {
	
	public void startup(String dbDir);
	
	public void shutdown();
	
	public void createGraph(String datasetDir);

}
