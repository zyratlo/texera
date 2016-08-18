package edu.uci.ics.textdb.dataflow.common;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;

/**
 * AbstractSingleInputOperator is an abstract class that can be used by many operators.
 * This class implements logic such as open and close input operator, manage cursor, manage limit and offset, etc..
 * Operators that extends this class must implement:
 * 
 *  setUp(). setUp() is called in open(). The purpose is to initialize resources, and build output schema.
 *  computeMatchingResults(). Compute a matching result of an input tuple according to operator's own semantic.
 *  cleanUp(). cleanUp() is called in close(). Close resources and set private variables to null.
 *  
 *  optional: getNextInputTuple(): return the next input tuple. The default implementation is inputOperator.getNextTuple().
 *      An operator can override it based on its own need.
 *  
 * @author Zuozhi Wang (zuozhiw)
 *
 */
public abstract class AbstractSingleInputOperator implements IOperator {
    
    protected IOperator inputOperator;
    protected Schema outputSchema;
    
    protected int cursor = CLOSED;
    
    protected int resultCursor = -1;
    protected int limit = Integer.MAX_VALUE;
    protected int offset = 0;
    
    @Override
    public void open() throws DataFlowException {
        if (cursor != CLOSED) {
            return;
        }
        try {
            if (this.inputOperator == null) {
                throw new DataFlowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
            }
            inputOperator.open();
            setUp();
            
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
        cursor = OPENED;
    }
    
    /**
     * SetUp necessary resources, variables in this function.
     * outputSchema MUST be initialized in setUP().
     * @throws DataFlowException
     */
    protected abstract void setUp() throws DataFlowException;

    @Override
    public ITuple getNextTuple() throws DataFlowException {
        if (cursor == CLOSED) {
            throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        if (resultCursor >= limit + offset - 1){
            return null;
        }
        try {
            ITuple inputTuple;
            ITuple resultTuple = null;
            while (true) {
                inputTuple = getNextInputTuple();
                if (inputTuple == null) {
                    break;
                }
                resultTuple = computeMatchingResults(inputTuple);
                if (resultTuple != null) {
                    cursor++;
                }
                if (resultTuple != null && cursor >= offset) {
                    break;
                }
            }
            return resultTuple;
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
    }
    
    protected ITuple getNextInputTuple() throws Exception {
        return inputOperator.getNextTuple();
    }
    
    /**
     * Given the inputTuple, compute the matching results of this operator, return null if the tuple doesn't match.
     * 
     * @param inputTuple
     * @return matching result, null if the tuple doesn't match
     * @throws DataFlowException
     */
    protected abstract ITuple computeMatchingResults(ITuple inputTuple) throws DataFlowException;

    @Override
    public void close() throws DataFlowException {
        if (cursor == CLOSED) {
            return;
        }
        try {
            if (inputOperator != null) {
                inputOperator.close();
            }
            cleanUp();
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
        cursor = CLOSED;
    }
    
    protected abstract void cleanUp() throws DataFlowException;

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }
    
    public void setInputOperator(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }
    
    public IOperator getInputOperator() {
        return inputOperator;
    }
    
    public void setLimit(int limit) {
        this.limit = limit;
    }
    
    public int getLimit() {
        return limit;
    }
    
    public void setOffset(int offset) {
        this.offset = offset;
    }
    
    public int getOffset() {
        return offset;
    }
    
}
