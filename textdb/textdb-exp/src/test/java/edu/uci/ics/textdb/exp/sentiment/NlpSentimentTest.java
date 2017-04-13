package edu.uci.ics.textdb.exp.sentiment;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSink;
import edu.uci.ics.textdb.exp.source.tuple.TupleSourceOperator;
import edu.uci.ics.textdb.exp.utils.DataflowUtils;

public class NlpSentimentTest {
    
    @Test
    public void test1() throws TextDBException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NlpSentimentTestConstants.POSITIVE_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        NlpSentimentOperator sentiment = new NlpSentimentOperator(
                new NlpSentimentPredicate(NlpSentimentTestConstants.TEXT, "sentiment"));
        TupleSink tupleSink = new TupleSink();
        
        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        
        System.out.println(DataflowUtils.getTupleListString(results));
    }
    

}
