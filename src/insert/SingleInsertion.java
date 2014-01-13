package insert;

import java.util.List;

public interface SingleInsertion {
	
	public void startup(String dbDir);
	
	public void shutdown();
	
	public List<Double> createGraph(String datasetDir);

}
