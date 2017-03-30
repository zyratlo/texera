package edu.uci.ics.textdb.exp.sampler;

import java.util.ArrayList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.constants.TestConstants;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.exp.sampler.SamplerPredicate.SampleType;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

/**
 * 
 * @author Qinhua Huang
 *
 */

public class SamplerTest {
    
    public static final String SAMPLER_TABLE = "sampler_test";
    private static int indexSize;
    @BeforeClass
    public static void setUp() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        // create the people table and write tuples
        
        RelationManager.getRelationManager().deleteTable(SAMPLER_TABLE);
        RelationManager relationManager2 = RelationManager.getRelationManager();
        relationManager2.createTable(SAMPLER_TABLE, "../index/test_tables/" + SAMPLER_TABLE, 
                TestConstants.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());
        DataWriter regexDataWriter = relationManager2.getTableDataWriter(SAMPLER_TABLE);
        regexDataWriter.open();
        indexSize = 0;
        for (Tuple tuple : TestConstants.getSamplePeopleTuples()) {
            regexDataWriter.insertTuple(tuple);
            indexSize ++;
        }
        regexDataWriter.close();
    }
    
    @AfterClass
    public static void cleanUp() throws TextDBException {
        RelationManager.getRelationManager().deleteTable(SAMPLER_TABLE);
    }
    
    public static List<Tuple> getSampleResults(String tableName, int k, SampleType sampleType ) throws TextDBException{
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(tableName);
        Sampler tupleSampler = new Sampler(new SamplerPredicate(k, sampleType));
        tupleSampler.setInputOperator(scanSource);
        
        List<Tuple> results = new ArrayList<>();
        Tuple tuple;
        
        tupleSampler.open();
        while((tuple = tupleSampler.getNextTuple()) != null) {
            results.add(tuple);
        }
        tupleSampler.close();
        return results;
    }
    
    
    /*
     * Sample the first-arrived 0 tuple in FIRST_K_ARRIVAL mode
     */
    
    @Test
    public void test1() throws TextDBException {
        List<Tuple> results = getSampleResults(SAMPLER_TABLE,0, SampleType.FIRST_K_ARRIVAL);
        Assert.assertEquals(results.size(), 0);
    }
    /*
     * Sample the first-arrived 1 tuple in FIRST_K_ARRIVAL mode.
     */
    
    @Test
    public void test2() throws TextDBException {
        List<Tuple> results = getSampleResults(SAMPLER_TABLE,1, SampleType.FIRST_K_ARRIVAL);
        Assert.assertEquals(results.size(), 1);
    }
    
    /*
     * Sample the all tuples in FIRST_K_ARRIVAL mode.
     */
    
    @Test
    public void test3() throws TextDBException {
        List<Tuple> results = getSampleResults(SAMPLER_TABLE,indexSize, SampleType.FIRST_K_ARRIVAL);
        Assert.assertEquals(results.size(), indexSize);
    }
    
    /*
     * Sample more tuples than the index contained in FIRST_K_ARRIVAL mode.
     */
    
    @Test
    public void test4() throws TextDBException {
        List<Tuple> results = getSampleResults(SAMPLER_TABLE,indexSize+1, SampleType.FIRST_K_ARRIVAL);
        Assert.assertEquals(results.size(), indexSize);
    }
    
    /*
     * Sample a middle number between [0, indexSize] in RANDOM_SAMPLE mode
     */
    @Test
    public void test5() throws TextDBException {
        List<Tuple> results = getSampleResults(SAMPLER_TABLE,2, SampleType.RANDOM_SAMPLE);
        Assert.assertEquals(results.size(), 2);

    }
    
    /*
     * Sample 0 tuples in RANDOM_SAMPLE mode.
     */
    @Test
    public void test6() throws TextDBException {
        List<Tuple> results = getSampleResults(SAMPLER_TABLE,0, SampleType.RANDOM_SAMPLE);
        Assert.assertEquals(results.size(), 0);
    }
    
    /*
     * Sample all tuples in RANDOM_SAMPLE mode.
     */
    @Test
    public void test7() throws TextDBException {
        List<Tuple> results = getSampleResults(SAMPLER_TABLE,indexSize, SampleType.RANDOM_SAMPLE);
        Assert.assertEquals(results.size(), indexSize);
    }
    
    /*
     * Sample more tuples than the index contained in RANDOM_SAMPLE mode.
     * It will return all the tuples in the index.
     */
    @Test
    public void test8() throws TextDBException {
        List<Tuple> results = getSampleResults(SAMPLER_TABLE,indexSize+1, SampleType.RANDOM_SAMPLE);
        Assert.assertEquals(results.size(), indexSize);
    }
}
