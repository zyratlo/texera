package edu.uci.ics.textdb.exp.source.tuple;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.constants.TestConstants;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IDField;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.TestUtils;
import edu.uci.ics.textdb.api.utils.Utils;

public class TupleSourceOperatorTest {
    
    @Test
    public void test1() throws TextDBException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                TestConstants.getSamplePeopleTuples(), TestConstants.SCHEMA_PEOPLE);
        
        tupleSource.open();
        
        Tuple tuple;
        List<Tuple> results = new ArrayList<>();
        Schema outputSchema = tupleSource.getOutputSchema();
        while ((tuple = tupleSource.getNextTuple()) != null) {
            results.add(tuple);
        }
        
        tupleSource.close();
        
        // assert result is equal to input
        Assert.assertTrue(TestUtils.equals(TestConstants.getSamplePeopleTuples(), results));
        // assert _id is added to schema
        Assert.assertTrue(outputSchema.equals(Utils.getSchemaWithID(TestConstants.SCHEMA_PEOPLE)));
        // assert all tuples contain _id field
        for (Tuple resultTuple : results) {
            Assert.assertTrue(resultTuple.getField(SchemaConstants._ID).getClass().equals(IDField.class));
        }
        
    }

}
