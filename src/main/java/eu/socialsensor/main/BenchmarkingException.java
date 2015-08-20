package eu.socialsensor.main;

public class BenchmarkingException extends RuntimeException
{

    /**
     * 
     */
    private static final long serialVersionUID = -4165548376731455231L;

    public BenchmarkingException(String message)
    {
        super(message);
    }

    public BenchmarkingException(String message, Throwable cause)
    {
        super(message, cause);
    }

}
