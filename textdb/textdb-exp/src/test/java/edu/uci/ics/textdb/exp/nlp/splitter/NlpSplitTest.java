package edu.uci.ics.textdb.exp.nlp.splitter;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

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
                new NlpSplitPredicate(NLPOutputType.ONE_TO_ONE, NlpSplitTestConstants.TEXT, "sentence_list"));
        TupleSink tupleSink = new TupleSink();
        
        sentence_list.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentence_list);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        for(Tuple result : results) {
            System.out.println(result.getField(1).toString());
        }
    }

}
