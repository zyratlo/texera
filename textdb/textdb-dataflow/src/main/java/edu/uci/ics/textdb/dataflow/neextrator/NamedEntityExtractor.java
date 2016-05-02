package edu.uci.ics.textdb.dataflow.neextrator;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;


/**
 * Created by Sam Hong on 16/4/21.
 */
public class NamedEntityExtractor implements IOperator{


    private IOperator sourceOperator;
    private ITuple sourceTuple;


    public NamedEntityExtractor(IOperator operator) {
        this.sourceOperator = operator;
    }


    /**
     * @about Opens Named Entity Extractor
     */
    @Override
    public void open() throws Exception {
        try {
            sourceOperator.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    /**
     * @about Return all named entities that are recognized in a document.
     *        Return format is a Tuple that contains only one field which is
     *        a List of span of the results
     *
     * @overview  First get a tuple from the source operator then process it
     *          using the Stanford NLP package. for all recognized words, compute their
     *          span and return all as a list.
     *
     */
    @Override
    public ITuple getNextTuple() throws Exception {
        return null;
    }


    /**
     * @about Closes the operator
     */
    @Override
    public void close() throws DataFlowException {
        try {
            sourceOperator.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
}
