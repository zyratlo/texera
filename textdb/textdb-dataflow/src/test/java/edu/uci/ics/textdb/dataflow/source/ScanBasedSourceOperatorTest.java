/**
 * 
 */
package edu.uci.ics.textdb.dataflow.source;

import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.constants.LuceneConstants;

/**
 * @author sandeepreddy602
 *
 */
public class ScanBasedSourceOperatorTest {
    
    private SampleDataStore dataStore;
    private ScanBasedSourceOperator scanBasedSourceOperator;
    
    @Before
    public void setUp() throws Exception{
        dataStore = new SampleDataStore();
        dataStore.clearData();
        dataStore.storeData();
        scanBasedSourceOperator = new ScanBasedSourceOperator(
                LuceneConstants.INDEX_DIR, TestConstants.SAMPLE_SCHEMA);
    }
    
    @After
    public void cleanUp() throws IOException{
        dataStore.clearData();
    }
    
    @Test
    public void testFlow() throws DataFlowException{
        scanBasedSourceOperator.open();
        ITuple nextTuple = null;
        int numTuples = 0;
        while((nextTuple  = scanBasedSourceOperator.getNextTuple()) != null){
            //Checking if the tuple retrieved is present in the samplesTuples
            boolean contains = contains(TestConstants.SAMPLE_TUPLES, nextTuple);
            Assert.assertTrue(contains);
            numTuples ++;
        }
        Assert.assertEquals(TestConstants.SAMPLE_TUPLES.size(), numTuples);
        scanBasedSourceOperator.close();
    }
    
    private boolean contains(List<ITuple> sampleTuples, ITuple actualTuple) {
        boolean contains = false;
        int schemaSize = TestConstants.SAMPLE_SCHEMA.size();
        for (ITuple sampleTuple : sampleTuples) {
            contains = true;
            for (int i = 0; i < schemaSize; i++) {
                if(!sampleTuple.getField(i).equals(actualTuple.getField(i))){
                    contains = false;
                }
            }
            if(contains){
                return contains;
            }
        }
        return contains;
    }
}
