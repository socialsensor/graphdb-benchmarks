package eu.socialsensor.main;

import static org.junit.Assert.fail;
import org.junit.Test;

public class GraphDatabaseBenchmarkTest
{
    @Test
    public void testGraphDatabaseBenchmark()
    {
        GraphDatabaseBenchmark bench = new GraphDatabaseBenchmark(null /* inputPath */);
        try
        {
            bench.run();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("Got unexpected exception: " + e.getMessage());
        }

        //bench.cleanup();
    }
}
