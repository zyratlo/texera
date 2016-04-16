package edu.uci.ics.textdb.storage;

import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.storage.reader.LuceneDataReader;
import edu.uci.ics.textdb.storage.writer.LuceneDataWriter;

public class LuceneDataWriterReaderTest {
    private IDataWriter dataWriter;
    private IDataReader dataReader;
    
    @Before
    public void setUp(){
        dataWriter = new LuceneDataWriter(LuceneDataStore.DATA_STORE_DIRECTORY);
        dataReader = new LuceneDataReader(
                LuceneDataStore.DATA_STORE_DIRECTORY, TestConstants.SAMPLE_SCHEMA_PEOPLE, 
                LuceneConstants.SCAN_QUERY, TestConstants.SAMPLE_SCHEMA_PEOPLE.get(0).getFieldName());
    }
    
    @Test
    public void testReadWriteData() throws Exception{
        dataWriter.clearData();
        List<Attribute> schema = TestConstants.SAMPLE_SCHEMA_PEOPLE;
        List<ITuple> tuples = TestConstants.getSamplePeopleTuples();
        dataWriter.writeData(schema, tuples);
        Assert.assertEquals(tuples.size(), LuceneDataStore.getNumDocuments());
        dataReader.open();
        ITuple nextTuple = null;
        int numTuples = 0;
        while((nextTuple  = dataReader.getNextTuple()) != null){
            //Checking if the tuple retrieved is present in the samplesTuples
            boolean contains = contains(tuples, nextTuple);
            Assert.assertTrue(contains);
            numTuples ++;
        }
        Assert.assertEquals(tuples.size(), numTuples);
        dataReader.close();
    }

    private boolean contains(List<ITuple> sampleTuples, ITuple actualTuple) {
        boolean contains = false;
        int schemaSize = TestConstants.SAMPLE_SCHEMA_PEOPLE.size();
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
