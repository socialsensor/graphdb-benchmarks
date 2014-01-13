package insert;

public interface MassiveInsertion {
	
	public void startup(String dbDir);
	
	public void shutdown();
	
	public void createGraph(String datasetDir);

}
