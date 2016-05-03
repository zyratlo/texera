package edu.uci.ics.textdb.dataflow.neextrator;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;

import java.util.List;


/**
 * @author Feng [sam0227] on 4/27/16.
 *
 *  Wrap the Stanford NLP Named Entity Recognizer as a operator.
 *  This operator would recognize 7 classes: Location, Person, Organization, Money, Percent, Date and Time.
 *  Return the recoginized data as a list of spans.
 *
 */

public class NamedEntityExtractor implements IOperator{


    private IOperator sourceOperator;
    private ITuple sourceTuple;
    private List<Attribute> searchInAttributes;



    public NamedEntityExtractor(IOperator operator) {
        this.sourceOperator = operator;
    }


    public NamedEntityExtractor(IOperator operator, List<Attribute> searchInAttributes) {
        this.sourceOperator = operator;
        this.searchInAttributes=searchInAttributes;
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
     *        a list of spans of the results
     *
     *        For example: Given tuple with two field named: sentence1, sentence2.
     *         tuple: ["Google is an organization.", "Its headquarter is in Mountain View."]
     *         return:
     *              ["sentence1,0,6,Google, ORGANIZATION", "sentence2,22,25,Mountain View, LOCATION"]
     *
     *
     * @overview  First get a tuple from the source operator then process it
     *          using the Stanford NLP package. for all recognized words, compute their
     *          spans and return all as a list.
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
