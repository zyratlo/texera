/**
 * 
 */
package edu.uci.ics.textdb.common.exception;

/**
 * @author sandeepreddy602
 *
 */
public class DataFlowException extends Exception{
    
    private static final long serialVersionUID = -4779329768850579335L;

    public DataFlowException(String errorMessage, Throwable throwable){
        super(errorMessage, throwable);
    }
    
    public DataFlowException(String errorMessage){
        super(errorMessage);
    }
}
