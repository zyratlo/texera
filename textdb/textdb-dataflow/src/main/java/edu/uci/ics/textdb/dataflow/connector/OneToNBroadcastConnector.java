package edu.uci.ics.textdb.dataflow.connector;

import java.util.ArrayList;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IConnector;
import edu.uci.ics.textdb.api.dataflow.IOperator;

/**
 * OneToNBroadcaastConnector connects one input operator with multiple output operators.
 * The tuples from the input operator will be broadcast to every output operator.
 * 
 * It is required that all output operators needs to be opened prior to calling getNextTuple().
 * @author Zuozhi Wang (zuozhiw)
 *
 */
public class OneToNBroadcastConnector implements IConnector {
    
    private int outputOperatorNumber;
    
    // A list of all child output operators
    private ArrayList<IOperator> outputOperatorList;
    // A list to maintain cursors of all operators
    private ArrayList<Integer> outputCursorList;
    // A list to maintain operators' status (opened or closed)
    private ArrayList<Boolean> outputOpenStatus;
    private boolean inputOperatorOpened;
    
    private IOperator inputOperator;
    private ArrayList<ITuple> inputTupleList;
    
    /**
     * Constructs a OneToNBroadcastConnector with n output operators.
     * @param n, the number of output operators this connector has
     */
    public OneToNBroadcastConnector(int n) {        
        this.inputTupleList = new ArrayList<>();
        inputOperatorOpened = false;
        this.outputOperatorNumber = n;
        intializeOutputOperators();
    }
    
    private void intializeOutputOperators() {
        this.outputOperatorList = new ArrayList<>();
        this.outputCursorList = new ArrayList<>();
        this.outputOpenStatus = new ArrayList<>();
        
        for (int i = 0; i < this.outputOperatorNumber; i++) {
            outputCursorList.add(-1);
            outputOpenStatus.add(false);
            outputOperatorList.add(new ConnectorOutputOperator(this, i));
        }
    }
    
    /**
     * Get the total number of output operators this connector has.
     */
    @Override
    public int getOutputNumber() {
        return outputOperatorNumber;
    }
    
    /**
     * Get the output operator corresponding to the index.
     * Index starts with 0. 
     * 0 corresponds to the first output operator, 1 corresponds to the second, etc.
     */
    @Override
    public IOperator getOutputOperator(int outputIndex) {
        return this.outputOperatorList.get(outputIndex);
    }
        
    /*
     * This returns the nextTuple of the operator corresponding to the index.
     * A cursor will be maintained for each operator. 
     * Tuples from input operators are cached in an in-memory list.
     * A new tuple will be fetched from input operator whenever a cursor exceeds the list.
     */
    private ITuple getNextTuple(int outputOperatorIndex) throws Exception {
        int nextPosition = outputCursorList.get(outputOperatorIndex) + 1;
        outputCursorList.set(outputOperatorIndex, nextPosition);
        
        if (nextPosition < inputTupleList.size()) {
            return inputTupleList.get(nextPosition);
        } else {
            ITuple nextInputTuple = inputOperator.getNextTuple();
            if (nextInputTuple == null) {
                return null;
            } else {
                inputTupleList.add(nextInputTuple);
                return nextInputTuple;
            }
        }
    }
    
    private void openInputOperator(int outputOperatorIndex) throws Exception {
        outputOpenStatus.set(outputOperatorIndex, true);
        if (! inputOperatorOpened) {
            inputOperator.open();
            inputOperatorOpened = true;
        }
    }
    
    private void closeConsumer(int outputOperatorIndex) throws Exception {
        outputOpenStatus.set(outputOperatorIndex, false);
        boolean isAllClosed = isAllClosed();
        if (isAllClosed) {
            inputOperator.close();
        }
    }
    
    /**
     * Set the input operator of this connector
     * @param inputOperator
     */
    public void setInputOperator(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }
    
    public IOperator getInputOperator() {
        return this.inputOperator;
    }

    private boolean isAllClosed() {
        return ! outputOpenStatus.stream().reduce(false, (a, b) -> (a || b));
    }
    
    
    private class ConnectorOutputOperator implements IOperator {
        
        private OneToNBroadcastConnector parentConnector;
        private int outputIndex;
                
        private ConnectorOutputOperator(OneToNBroadcastConnector parentConnector, int outputIndex) {
            this.parentConnector = parentConnector;
            this.outputIndex = outputIndex;
        }

        @Override
        public void open() throws Exception {
            parentConnector.openInputOperator(outputIndex);
        }

        @Override
        public ITuple getNextTuple() throws Exception {
            return parentConnector.getNextTuple(outputIndex);
        }

        @Override
        public void close() throws Exception {      
            parentConnector.closeConsumer(outputIndex);
        }

        @Override
        public Schema getOutputSchema() {
            return parentConnector.getInputOperator().getOutputSchema();
        }
        
    }
   
}
