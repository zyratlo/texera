package edu.uci.ics.texera.exp.nlp.splitter;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.exp.utils.DataflowUtils;
import edu.uci.ics.texera.exp.common.PropertyNameConstants;

public class NlpSplitTestConstants {
    
    public static String TEXT = "text";
    
    public static Attribute TEXT_ATTRIBUTE = new Attribute("text", AttributeType.TEXT);
    public static Schema SPLIT_SCHEMA = new Schema(TEXT_ATTRIBUTE);
    
    private static String sentence1 =  "1.3 billion equals 1300 million .";
    private static String sentence2 =  "Mr. Wayne does not split .";
    

    
    public static List<Tuple> getOneToOneTestTuple() throws ParseException {
        IField[] fields1 = { new TextField(sentence1 + sentence2) };
        Tuple tuple1 = new Tuple(SPLIT_SCHEMA, fields1);
        return Arrays.asList(tuple1);
    }
    
    public static List<Tuple> getOneToManyTestTuple() throws ParseException {
        IField[] fields1 = { new TextField(sentence1 + sentence2) };
        Tuple tuple1 = new Tuple(SPLIT_SCHEMA, fields1);
        return Arrays.asList(tuple1);
    }
    
    public static List<Tuple> getOneToOneResultTuple() throws ParseException {
        
        // Build the expected result Tuple
        List<Tuple> resultList = new ArrayList<>();
        List<Span> spanList = new ArrayList<Span>();
        
        Span span1 = new Span(TEXT, 0, sentence1.length(), PropertyNameConstants.NLP_SPLIT_KEY, sentence1);
        spanList.add(span1);
        Span span2 = new Span(TEXT, sentence1.length()+1, sentence1.length()+sentence2.length()+1, PropertyNameConstants.NLP_SPLIT_KEY, sentence2);
        spanList.add(span2);

        Tuple tuple1 = getOneToOneTestTuple().get(0);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        Tuple returnTuple = DataflowUtils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }
    
    public static List<Tuple> getOneToManyResultTuple() throws ParseException {
        
        // Build the expected result Tuple
        List<Tuple> resultList = new ArrayList<>();
        
        IField sentenceText1 = new TextField(sentence1);
        Tuple tuple1 = getOneToManyTestTuple().get(0);
        Schema returnSchema1 = Utils.addAttributeToSchema(tuple1.getSchema(), new Attribute(PropertyNameConstants.NLP_OUTPUT_TYPE, AttributeType.TEXT));
 
        List<IField> outputFields1 = new ArrayList<>();
        outputFields1 = tuple1.getFields();
        outputFields1.add(sentenceText1);
        
        Tuple returnTuple1 = new Tuple(returnSchema1, outputFields1);
        resultList.add(returnTuple1);
        
        
        IField sentenceText2 = new TextField(sentence2);
        Tuple tuple2 = getOneToManyTestTuple().get(0);
        Schema returnSchema2 = Utils.addAttributeToSchema(tuple2.getSchema(), new Attribute(PropertyNameConstants.NLP_OUTPUT_TYPE, AttributeType.TEXT));
 
        List<IField> outputFields2 = new ArrayList<>();
        outputFields2 = tuple2.getFields();
        outputFields2.add(sentenceText2);
        
        Tuple returnTuple2 = new Tuple(returnSchema2, outputFields2);
        resultList.add(returnTuple2);      

        return resultList;
    }
}
