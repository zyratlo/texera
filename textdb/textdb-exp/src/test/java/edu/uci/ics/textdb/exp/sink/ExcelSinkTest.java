package edu.uci.ics.textdb.exp.sink;

import java.io.FileNotFoundException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.constants.TestConstants;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.field.DateField;
import edu.uci.ics.textdb.api.field.DoubleField;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import junit.framework.Assert;

public class ExcelSinkTest {
    
    private ExcelSink excelSink;
    private IOperator inputOperator;
    private Schema inputSchema = new Schema(
            SchemaConstants._ID_ATTRIBUTE, new Attribute("content", AttributeType.TEXT), SchemaConstants.PAYLOAD_ATTRIBUTE);
    
    @Before
    public void setUp() throws FileNotFoundException {
        inputOperator = Mockito.mock(IOperator.class);
        Mockito.when(inputOperator.getOutputSchema()).thenReturn(inputSchema);
        excelSink = new ExcelSink();
        excelSink.setInputOperator(inputOperator);
    }

    @After
    public void cleanUp() {
    }

    @Test
    public void testOpen() throws Exception {
    	excelSink.open();
//        // verify that inputOperator called open() method
        Mockito.verify(inputOperator).open();
//        // assert that the tuple stream sink removes the _ID and PAYLOAD attribute
        Assert.assertEquals(new Schema(new Attribute("content", AttributeType.TEXT)), excelSink.getOutputSchema());
    
    	excelSink.close();
        excelSink.deleteFile();
    }

    @Test
    public void testClose() throws Exception {
    	excelSink.open();
    	excelSink.close();
        // verify that inputOperator called close() method
        Mockito.verify(inputOperator).close();
        excelSink.deleteFile();
    }

    @Test
    public void testGetNextTuple() throws Exception {
        Tuple sampleTuple = Mockito.mock(Tuple.class);
        Mockito.when(sampleTuple.toString()).thenReturn("Sample Tuple");
        Mockito.when(sampleTuple.getSchema()).thenReturn(inputSchema);
        // Set the behavior for inputOperator,
        // first it returns some non-null tuple and second time it returns null
        Mockito.when(inputOperator.getNextTuple()).thenReturn(sampleTuple).thenReturn(null);
        
        excelSink.open();
        excelSink.getNextTuple();
        
        // Verify that input operator's getNextTuple is called
        Mockito.verify(inputOperator, Mockito.times(1)).getNextTuple();

        excelSink.close();
        excelSink.deleteFile();
    }
    
    /**
     * Create two tuples, write into a excel file. Need to manually delete the generated file.
     * @throws ParseException
     */
    @Test
    public void writeSampleExcelFile() throws ParseException{
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        attributeNames.add(TestConstants.LAST_NAME);
        attributeNames.add(TestConstants.DESCRIPTION);

        // Prepare the expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span("firstName", 0, 5, "bruce1", "bruce1");
        Span span2 = new Span("firstName", 0, 5, "bruce2", "bruce2");
        list.add(span1);
        list.add(span2);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce1"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<>(list) };
        IField[] fields2 = { new StringField("bruce2"), new StringField("john2 Lee"), new IntegerField(0),
                new DoubleField(6.0), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1994")),
                new TextField("Angry Bird"), new ListField<>(list) };
        
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        Tuple tuple2 = new Tuple(new Schema(schemaAttributes), fields2);

        IOperator inputOperator = Mockito.mock(IOperator.class);
        Mockito.when(inputOperator.getOutputSchema()).thenReturn(new Schema(schemaAttributes)).thenReturn(null);
        Mockito.when(inputOperator.getNextTuple()).thenReturn(tuple1).thenReturn(tuple2).thenReturn(null);

    	excelSink = new ExcelSink();
        excelSink.setInputOperator(inputOperator);
        excelSink.open();
        excelSink.collectAllTuples();
        excelSink.close();
//        excelSink.deleteFile();
    }
    
}
