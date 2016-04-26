package edu.uci.ics.textdb.common.field;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class SpanTest {
    
    private Span span;
    
    @Before
    public void setUp(){
        
    }
    
    @Test
    public void testGetters(){
        int start = 10;
        int end = 20;
        String fieldName = "description";
        String key = "location";
        String value = "new york";
        span = new Span(fieldName, key, value, start, end);
        Assert.assertEquals(start, span.getStart());
        Assert.assertEquals(end, span.getEnd());
        Assert.assertEquals(key, span.getKey());
        Assert.assertEquals(value, span.getValue());
        Assert.assertEquals(fieldName, span.getFieldName());
    }
}
