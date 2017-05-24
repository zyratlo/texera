package edu.uci.ics.textdb.exp.nlp.splitter;

import java.text.ParseException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.TestUtils;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSink;
import edu.uci.ics.textdb.exp.source.tuple.TupleSourceOperator;


public class NlpSplitTest {
    
    @Test
    public void test1() throws TextDBException, ParseException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                NlpSplitTestConstants.getOneToOneTestTuple(), NlpSplitTestConstants.SPLIT_SCHEMA);
        NlpSplitOperator sentence_list = new NlpSplitOperator(
                new NlpSplitPredicate(NLPOutputType.ONE_TO_ONE, NlpSplitTestConstants.TEXT, SchemaConstants.SPAN_LIST));
        TupleSink tupleSink = new TupleSink();
        
        sentence_list.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentence_list);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        Assert.assertTrue(TestUtils.equals(NlpSplitTestConstants.getOneToOneResultTuple(), results));
    }
    
    @Test
    public void test2() throws TextDBException, ParseException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                NlpSplitTestConstants.getOneToManyTestTuple(), NlpSplitTestConstants.SPLIT_SCHEMA);
        NlpSplitOperator sentence_list = new NlpSplitOperator(
                new NlpSplitPredicate(NLPOutputType.ONE_TO_MANY, NlpSplitTestConstants.TEXT, PropertyNameConstants.NLP_OUTPUT_TYPE));
        TupleSink tupleSink = new TupleSink();
        
        sentence_list.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentence_list);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        Assert.assertTrue(TestUtils.equals(NlpSplitTestConstants.getOneToManyResultTuple(), results));
    }
}
