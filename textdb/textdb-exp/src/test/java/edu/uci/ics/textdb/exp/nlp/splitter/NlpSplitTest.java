package edu.uci.ics.textdb.exp.nlp.splitter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.stanford.nlp.international.Language;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSink;
import edu.uci.ics.textdb.exp.source.tuple.TupleSourceOperator;

public class NlpSplitTest {

    @Test
    public void test1() throws TextDBException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NlpSplitTestConstants.MULTI_SENTENCE_TUPLE), NlpSplitTestConstants.SPLIT_SCHEMA);
        NlpSplitOperator sentence_list = new NlpSplitOperator(
                new NlpSplitPredicate(Language.French, NLPOutputType.ONE_TO_ONE, NlpSplitTestConstants.TEXT, "sentence_list"));
        TupleSink tupleSink = new TupleSink();
        
        sentence_list.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentence_list);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        for(Tuple result : results) {
            System.out.println(result);
        }
        
//        Tuple tuple = results.get(0);
//        ArrayList<String> resultsList = (ArrayList<String>)tuple.getField(1).getValue();
//        System.out.println(resultsList.size());
//        System.out.println(tuple.getField(1).getValue());
//        Assert.assertEquals(tuple.getField(2).getValue().getClass(), ArrayList.class);
    }

}
