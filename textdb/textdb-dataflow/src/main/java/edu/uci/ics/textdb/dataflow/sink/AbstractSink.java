package edu.uci.ics.textdb.dataflow.sink;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;

/**
 * Created by chenli on 5/11/16.
 */
public abstract class AbstractSink implements ISink {

    private IOperator childOperator;

    public AbstractSink(IOperator childOperator) {
        this.childOperator = childOperator;
    }

    /**
     * @about Opens the child operator.
     */
    @Override
    public void open() throws Exception {
        childOperator.open();
    }

    @Override
    public void processTuples() throws Exception {

        ITuple nextTuple;

        while ((nextTuple = childOperator.getNextTuple()) != null) {
            processOneTuple(nextTuple);
        }

    }

    /**
     *
     * @param nextTuple A tuple that needs to be processed during each iteration
     */
    protected abstract void processOneTuple(ITuple nextTuple);

    @Override
    public void close() throws Exception {
        childOperator.close();
    }
}
