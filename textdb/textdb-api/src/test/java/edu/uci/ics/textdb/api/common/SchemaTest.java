package edu.uci.ics.textdb.api.common;

import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class SchemaTest {
    private Schema schema;
    private String fieldName_1 = "sampleField_1";
    private FieldType type_1 = FieldType.STRING;
    private String fieldName_2 = "sampleField_2";
    private FieldType type_2 = FieldType.STRING;

    private Attribute[] attributes = 
        {new Attribute(fieldName_1, type_1), new Attribute(fieldName_2, type_2)};;
    
    @Before
    public void setUp(){
        schema = new Schema(attributes);
    }
    
    @Test
    public void testGetAttributes(){
        List<Attribute> attributes = schema.getAttributes();
        Assert.assertEquals(this.attributes.length, attributes.size());
        for(Attribute attribute : this.attributes){
            Assert.assertTrue(attributes.contains(attribute));
        }
    }
    
    @Test
    public void testGetIndex(){
        
        int expectedIndex_1 = 0;
        int expectedIndex_2 = 1;
        int retrievedIndex_1 = schema.getIndex(fieldName_1.toUpperCase());
        int retrievedIndex_2 = schema.getIndex(fieldName_2);
        Assert.assertEquals(expectedIndex_1, retrievedIndex_1);
        Assert.assertEquals(expectedIndex_2, retrievedIndex_2);
        
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testAddingNewAttribute(){ //Should fail
        List<Attribute> attributes  = schema.getAttributes();
        attributes.add(new Attribute("sampleField_3", FieldType.STRING));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testRemovingAttribute(){ //Should fail
        List<Attribute> attributes  = schema.getAttributes();
        attributes.remove(0);
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testAddingInBetween(){ //Should fail
        List<Attribute> attributes  = schema.getAttributes();
        attributes.add(0, new Attribute("sampleField_3", FieldType.STRING));
    }
}
