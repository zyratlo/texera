package edu.uci.ics.textdb.dataflow.connector;

import java.util.ArrayList;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;

public class OneToManyCacheConnector implements IOperator {
    
    private IOperator inputOperator;
    private Schema outputSchema;
    
    private ArrayList<ITuple> inputTupleList = new ArrayList<>();
    
    private boolean isOpen = false;
    private boolean inputAllConsumed = false;
    private int cursor = 0;

    @Override
    public void open() throws TextDBException {
        if (isOpen) {
            return;
        }
        if (inputOperator == null) {
            throw new DataFlowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputOperator.open();
        outputSchema = inputOperator.getOutputSchema();
        isOpen = true;
    }

    @Override
    public ITuple getNextTuple() throws TextDBException {
        if (! isOpen) {
            throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        // if cursor's next value exceeds the cache's size
        if (cursor + 1 >= inputTupleList.size()) {
            // if the input operator has been consumed, return null
            if (inputAllConsumed) {
                return null;
            // else, get the next tuple from input operator, 
            // add it to tuple list, and advance cursor
            } else {
                ITuple tuple = inputOperator.getNextTuple();
                if (tuple == null) {
                    inputAllConsumed = true;
                } else {
                    inputTupleList.add(tuple);
                    cursor++;
                }
                return tuple;
            }
        // if we can get the tuple from the cache, retrieve it and advance cursor
        } else {
            ITuple tuple = inputTupleList.get(cursor);
            cursor++;
            return tuple;
        }
    }

    @Override
    public void close() throws TextDBException {
        if (! isOpen) {
            return;
        }
        // if the inputOperator's tuples have all been consumed
        // then it's safe to close it
        if (inputAllConsumed) {
            inputOperator.close();
        }
        // reset the cursor
        cursor = 0;
    }
    
    public void closeCache() throws TextDBException {
        if (! isOpen) {
            return;
        }
        inputAllConsumed = true;
        isOpen = false;
        cursor = 0;
        inputOperator.close();
    }

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }
    
    public void setInputOperator(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }

}
