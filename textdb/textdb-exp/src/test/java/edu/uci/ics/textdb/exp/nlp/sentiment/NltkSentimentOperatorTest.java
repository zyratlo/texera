package edu.uci.ics.textdb.exp.nlp.sentiment;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.nlp.sentiment.NlpSentimentTestConstants;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSink;
import edu.uci.ics.textdb.exp.source.tuple.TupleSourceOperator;

public class NltkSentimentOperatorTest {
    private static String NEGATIVE = "neg";
    private static String POSTIVE = "pos";
    /*
     * Test sentiment test result should be negative.
     */
    @Test
    public void test1() throws TextDBException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NlpSentimentTestConstants.POSITIVE_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        NltkSentimentOperator sentiment = new NltkSentimentOperator(
                new NltkSentimentOperatorPredicate(NlpSentimentTestConstants.TEXT, "sentiment", 1000));
        TupleSink tupleSink = new TupleSink();
        
        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        
        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), NEGATIVE);
    }
    
    /*
     * Test result should be positive.
     */
    @Test
    public void test2() throws TextDBException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NlpSentimentTestConstants.NEUTRAL_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        NltkSentimentOperator sentiment = new NltkSentimentOperator(
                new NltkSentimentOperatorPredicate(NlpSentimentTestConstants.TEXT, "sentiment",1000));
        TupleSink tupleSink = new TupleSink();
        
        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        
        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), POSTIVE);
    }
    
    /*
     * Test sentiment test result should be negative
     */
    @Test
    public void test3() throws TextDBException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NlpSentimentTestConstants.NEGATIVE_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        NltkSentimentOperator sentiment = new NltkSentimentOperator(
                new NltkSentimentOperatorPredicate(NlpSentimentTestConstants.TEXT, "sentiment", 1000));
        
        TupleSink tupleSink = new TupleSink();
        
        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        
        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), NEGATIVE);        
    }

}
