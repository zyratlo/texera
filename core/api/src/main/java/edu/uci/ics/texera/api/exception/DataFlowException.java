/**
 * 
 */
package edu.uci.ics.texera.api.exception;

/**
 *  Thrown to indicate that an exception occurs when a Texera operator processes data.
 */
public class DataFlowException extends TexeraException {

    private static final long serialVersionUID = -4779329768850579335L;

    public DataFlowException(String errorMessage, Throwable throwable) {
        super(errorMessage, throwable);
    }

    public DataFlowException(String errorMessage) {
        super(errorMessage);
    }
    
    public DataFlowException(Throwable throwable) {
        super(throwable);
    }
    
}
