package edu.uci.ics.textdb.storage;

import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.field.Attribute;

public class LuceneDataStoreTest {
    private LuceneDataStore dataStore;
    
    @Before
    public void setUp(){
        dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR);
    }
    
    @Test
    public void testStoreData() throws Exception{
        dataStore.clearData();
        List<Attribute> schema = TestConstants.SAMPLE_SCHEMA;
        List<ITuple> tuples = TestConstants.getSampleTuples();
        dataStore.storeData(schema, tuples);
        
        //Using SampleScanBased Operator since we cannot use ScanBasedSourceOperator present in textdb-dataflow project.
        //If textdb-storage references textdb-dataflow it creates a cyclic dependency.
        IOperator operator = new SampleScanBasedOperator(LuceneConstants.INDEX_DIR, TestConstants.SAMPLE_SCHEMA);
        operator.open();
        ITuple tuple = null;
        int numTuples = 0;
        while((tuple = operator.getNextTuple()) != null){
            numTuples++;
        }
        Assert.assertEquals(tuples.size(), numTuples);
        operator.close();
    }
}
