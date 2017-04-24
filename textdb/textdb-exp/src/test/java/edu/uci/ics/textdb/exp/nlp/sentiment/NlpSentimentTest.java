package edu.uci.ics.textdb.exp.nlp.sentiment;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSink;
import edu.uci.ics.textdb.exp.source.tuple.TupleSourceOperator;

public class NlpSentimentTest {
    
    /*
     * Test sentiment with a positive sentence, result should be 3 (positive)
     */
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
        
        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), 3);
    }
    
    /*
     * Test sentiment with a neutral sentence, result should be 2 (neutral)
     */
    @Test
    public void test2() throws TextDBException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NlpSentimentTestConstants.NEUTRAL_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        NlpSentimentOperator sentiment = new NlpSentimentOperator(
                new NlpSentimentPredicate(NlpSentimentTestConstants.TEXT, "sentiment"));
        TupleSink tupleSink = new TupleSink();
        
        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        
        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), 2);     
    }
    
    /*
     * Test sentiment with a negative sentence, result should be 1 (negative)
     */
    @Test
    public void test3() throws TextDBException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NlpSentimentTestConstants.NEGATIVE_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        NlpSentimentOperator sentiment = new NlpSentimentOperator(
                new NlpSentimentPredicate(NlpSentimentTestConstants.TEXT, "sentiment"));
        TupleSink tupleSink = new TupleSink();
        
        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        
        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), 1);        
    }  

}
