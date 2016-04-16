package edu.uci.ics.textdb.dataflow.source;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;

public class LuceneScanOperator implements ISourceOperator {
    @Override
    /*
    It's useful to have Boolean as the return type of open and close. Or handle the output using Exceptions if that's more Java style
     */
    public void open() {

    }

    @Override
    /*
    It's useful if we pass a structure like 'IParameters' to all the three methods using which the called can pass
    runtime parameters ...
     */
    public ITuple getNextTuple() {
        return null;
    }

    @Override
    public void close() {

    }
}
