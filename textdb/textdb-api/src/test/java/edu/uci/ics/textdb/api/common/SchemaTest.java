package edu.uci.ics.textdb.api.common;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class SchemaTest {
    private Schema schema;
    private List<Attribute> attributes;
    private String fieldName = "sampleField";
    private FieldType type = FieldType.STRING;
    
    @Before
    public void setUp(){
        attributes = new ArrayList<Attribute>();
        attributes.add(new Attribute(fieldName, type));
        schema = new Schema(attributes);
    }
    
    @Test
    public void testGetAttributes(){
        List<Attribute> attributes = schema.getAttributes();
        Assert.assertSame(this.attributes, attributes);
    }
}
