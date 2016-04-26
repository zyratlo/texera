package edu.uci.ics.textdb.common.field;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;

public class SpanTupleTest {
    
    private ITuple spanTuple;
    
    @Before
    public void setUp(){
        
    }
    
    @Test
    public void testGetters() throws ParseException{
        //create data tuple first
        List<Attribute> attributes = new ArrayList<Attribute>(TestConstants.ATTRIBUTES_PEOPLE);
        List<IField> fields = new ArrayList<IField>(
                Arrays.asList(new IField[]{new StringField("bruce"), new StringField("lee"), new IntegerField(46), 
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("bruce was born in new york city and was grown up in los angeles")}));
        attributes.add(SchemaConstants.SPAN_LIST_ATTRIBUTE);
        
        IField spanField = createSpanListField();
        fields.add(spanField);
        spanTuple = new DataTuple(new Schema(attributes), fields.toArray(new IField[fields.size()]));
        
        IField spanFieldRetrieved = spanTuple.getField(SchemaConstants.SPAN_LIST);
        
        Assert.assertTrue(spanFieldRetrieved instanceof ListField);
        Assert.assertSame(spanField, spanFieldRetrieved);
        
    }

    private IField createSpanListField() {
        List<Span> list = new ArrayList<Span>();
        //The key value will be:
        //For RegexMatcher : "n.*k"
        //For NamedEntityMatcher : LOCATION
        //For DictionaryMatcher: "new york" - For DictionaryMatcher the key and value are same
        Span span1 = new Span("description", "LOCATION", "new york", 18, 26);
        Span span2 = new Span("description", "LOCATION", "los angeles", 52, 63);
        list.add(span1);
        list.add(span2);
        IField spanListField = new ListField<Span>(list );
        return spanListField;
    }
    
}
