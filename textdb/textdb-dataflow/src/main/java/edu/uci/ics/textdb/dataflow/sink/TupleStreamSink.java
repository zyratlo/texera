package edu.uci.ics.textdb.dataflow.sink;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.utils.Utils;

public class TupleStreamSink implements ISink {
    
    private IOperator inputOperator;
    
    private Schema inputSchema;
    private Schema outputSchema;
    
    private boolean isOpen = false;;
    
    public TupleStreamSink() {
    }
    
    public void setInputOperator(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    @Override
    public void open() throws TextDBException {
        if (isOpen) {
            return;
        }     
        inputOperator.open();
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = Utils.removeAttributeFromSchema(inputSchema, SchemaConstants._ID, SchemaConstants.PAYLOAD);
        isOpen = true;
    }

    @Override
    public void processTuples() throws TextDBException {
        return;
    }
    
    @Override
    public ITuple getNextTuple() throws TextDBException {
        ITuple tuple = inputOperator.getNextTuple();
        if (tuple == null) {
            return null;
        }
        return null;
    }

    @Override
    public void close() throws TextDBException {
        if (! isOpen) {
        }
        inputOperator.close();
        isOpen = false;        
    }

}
