package edu.uci.ics.textdb.textql.statements.predicates;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.common.constants.SchemaConstants;

/**
 * This class contains test cases for the SelectSomeFieldsPredicate class.
 * The constructor, getters, setters and the getOperatorBean methods are
 * tested.
 * 
 * @author Flavio Bayer
 *
 */
public class SelectSomeFieldsPredicateTest {
    
    /**
     * Test the class constructor, getters and the setter methods.
     * Call the constructor of the SelectSomeFieldsPredicate, test 
     * if the returned value by the getter is the same as used in 
     * the constructor and then test if the value is changed
     * when the setter method is invoked.
     */
    @Test
    public void testConstructorsGettersSetters(){
        List<String> projectedFields;

        projectedFields = Collections.emptyList();
        assertConstructorGettersSetters(projectedFields);
        
        projectedFields = Arrays.asList("a","b","c","d");
        assertConstructorGettersSetters(projectedFields);
        
        projectedFields = Arrays.asList("field1", "field2", "field0");
        assertConstructorGettersSetters(projectedFields);        

        projectedFields = Arrays.asList(SchemaConstants._ID, SchemaConstants.PAYLOAD, SchemaConstants.SPAN_LIST);
        assertConstructorGettersSetters(projectedFields);
    }
    
    /**
     * Assert the correctness of the Constructor, getter and setter methods.
     * @param projectedFields The list of projected fields to be tested.
     */
    private void assertConstructorGettersSetters(List<String> projectedFields){
        SelectSomeFieldsPredicate selectSomeFieldsPredicate;
        
        // Check constructor
        selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(projectedFields);
        Assert.assertEquals(selectSomeFieldsPredicate.getProjectedFields(), projectedFields);
        
        // Check set projectedFields to null
        selectSomeFieldsPredicate.setProjectedFields(null);
        Assert.assertEquals(selectSomeFieldsPredicate.getProjectedFields(), null);
        
        // Check set projectedFields to the given list of fields
        selectSomeFieldsPredicate.setProjectedFields(projectedFields);
        Assert.assertEquals(selectSomeFieldsPredicate.getProjectedFields(), projectedFields);
    }
    
}
