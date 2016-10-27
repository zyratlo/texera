package edu.uci.ics.textdb.common.exception;

/**
 * @author sweetest
 *
 */
public class PlanStorageException extends Exception {

    public PlanStorageException(String errorMessage, Throwable throwable) {
        super(errorMessage, throwable);
    }

    public PlanStorageException(String errorMessage) {
        super(errorMessage);
    }

}
