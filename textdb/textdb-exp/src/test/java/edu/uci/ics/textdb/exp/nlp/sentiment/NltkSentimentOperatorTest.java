package edu.uci.ics.textdb.exp.nlp.sentiment;

import java.util.ArrayList;
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
    private static String MODEL = "Senti.pickle";
    private static int BUFFERSIZE = 1000;
    /*
     * Test sentiment test result should be negative.
     */
    @Test
    public void test1() throws TextDBException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NlpSentimentTestConstants.POSITIVE_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        NltkSentimentOperator sentiment = new NltkSentimentOperator(
                new NltkSentimentOperatorPredicate(NlpSentimentTestConstants.TEXT, "sentiment", BUFFERSIZE, MODEL));
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
                new NltkSentimentOperatorPredicate(NlpSentimentTestConstants.TEXT, "sentiment", BUFFERSIZE, null));
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
                new NltkSentimentOperatorPredicate(NlpSentimentTestConstants.TEXT, "sentiment", BUFFERSIZE, MODEL));
        
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
     * Test batch process, all test results should be negative
     */
    @Test
    public void test4() throws TextDBException {
        int bufferSize = 30;
        int tupleSize = 101;
        
        List<Tuple> listTuple = new ArrayList<>();
        for (int i=0; i<tupleSize; i++) {
            listTuple.add(NlpSentimentTestConstants.NEGATIVE_TUPLE);
        }
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                listTuple, NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        NltkSentimentOperator sentiment = new NltkSentimentOperator(
                new NltkSentimentOperatorPredicate(NlpSentimentTestConstants.TEXT, "sentiment", bufferSize, MODEL));
        
        TupleSink tupleSink = new TupleSink();
        
        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        for (int j=0; j<tupleSize; j++) {
            Tuple tuple = results.get(j);
            Assert.assertEquals(tuple.getField("sentiment").getValue(), NEGATIVE);
        }
    }

}
