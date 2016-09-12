package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.DictionaryMatcherSourceOperator;
import junit.framework.Assert;

public class DictionarySourceBuilderTest {
    
    @Test
    public void testDictionarySourceBuilder1() throws Exception {
        String directoryStr = "./index";
        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");
                
        List<Attribute> schemaAttrs = Arrays.asList(
                new Attribute("id", FieldType.INTEGER),
                new Attribute("city", FieldType.STRING),
                new Attribute("location", FieldType.STRING),
                new Attribute("content", FieldType.TEXT));
        
        String dictionaryStr = "Irvine, Anaheim, Costa Mesa, Santa Ana";
        List<String> dictionaryList = Arrays.asList(
                "Irvine",
                "Anaheim",
                "Costa Mesa",
                "Santa Ana");
        List<Attribute> dictAttrs = schemaAttrs.stream()
                .filter(attr -> ! attr.getFieldName().equals("id")).collect(Collectors.toList());

        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(DictionaryMatcherBuilder.DICTIONARY, dictionaryStr);
        operatorProperties.put(DictionaryMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
        operatorProperties.put(OperatorBuilderUtils.DATA_DIRECTORY, directoryStr);
        operatorProperties.put(OperatorBuilderUtils.SCHEMA, schemaJsonJSONObject.toString());
        
        DictionaryMatcherSourceOperator sourceOperator = DictionarySourceBuilder.buildSourceOperator(operatorProperties);

        
        String dictionaryEntry = null;
        ArrayList<String> actualDictionary = new ArrayList<>();
        while ((dictionaryEntry = sourceOperator.getPredicate().getNextDictionaryEntry()) != null) {
            actualDictionary.add(dictionaryEntry);
        }
        
        // compare dict
        Assert.assertEquals(dictionaryList, actualDictionary);
        // compare dataStore directory
        Assert.assertEquals(directoryStr, sourceOperator.getDataStore().getDataDirectory());
        // compare dataStore schema
        Assert.assertEquals(
                schemaAttrs.stream().collect(Collectors.toList()).toString(), 
                sourceOperator.getDataStore().getSchema().getAttributes().stream().collect(Collectors.toList()).toString());
        // compare dictMatcher attribute list
        Assert.assertEquals(dictAttrs.toString(), sourceOperator.getPredicate().getAttributeList().toString());

    }
    
    /*
     * Test invalid DictionaryMatcherSourceOperator with invalid attribute type
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidBuilder1() throws Exception {
        String directoryStr = "./index";
        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "random_type, string, type-that-doesnt-exist, text");
         
        String dictionaryStr = "Irvine, Anaheim, Costa Mesa, Santa Ana";
   
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(DictionaryMatcherBuilder.DICTIONARY, dictionaryStr);
        operatorProperties.put(DictionaryMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
        operatorProperties.put(OperatorBuilderUtils.DATA_DIRECTORY, directoryStr);
        operatorProperties.put(OperatorBuilderUtils.SCHEMA, schemaJsonJSONObject.toString());
        
        DictionaryMatcherSourceOperator sourceOperator = DictionarySourceBuilder.buildSourceOperator(operatorProperties);
    }
    
    /*
     * Test invalid DictionaryMatcherSourceOperator with inconsistent attribute names and types
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidBuilder2() throws Exception {
        String directoryStr = "./index";
        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, text");
         
        String dictionaryStr = "Irvine, Anaheim, Costa Mesa, Santa Ana";
   
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(DictionaryMatcherBuilder.DICTIONARY, dictionaryStr);
        operatorProperties.put(DictionaryMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
        operatorProperties.put(OperatorBuilderUtils.DATA_DIRECTORY, directoryStr);
        operatorProperties.put(OperatorBuilderUtils.SCHEMA, schemaJsonJSONObject.toString());
        
        DictionaryMatcherSourceOperator sourceOperator = DictionarySourceBuilder.buildSourceOperator(operatorProperties);
    }
    
    /*
     * Test invalid DictionaryMatcherSourceOperator with empty directory
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidBuilder3() throws Exception {
        String directoryStr = "";
        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, text");
         
        String dictionaryStr = "Irvine, Anaheim, Costa Mesa, Santa Ana";
   
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(DictionaryMatcherBuilder.DICTIONARY, dictionaryStr);
        operatorProperties.put(DictionaryMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
        operatorProperties.put(OperatorBuilderUtils.DATA_DIRECTORY, directoryStr);
        operatorProperties.put(OperatorBuilderUtils.SCHEMA, schemaJsonJSONObject.toString());
        
        DictionaryMatcherSourceOperator sourceOperator = DictionarySourceBuilder.buildSourceOperator(operatorProperties);
    }
    
    /*
     * Test invalid DictionaryMatcherSourceOperator with another empty directory
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidBuilder4() throws Exception {
        String directoryStr = "      ";
        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, text");
         
        String dictionaryStr = "Irvine, Anaheim, Costa Mesa, Santa Ana";
   
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(DictionaryMatcherBuilder.DICTIONARY, dictionaryStr);
        operatorProperties.put(DictionaryMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
        operatorProperties.put(OperatorBuilderUtils.DATA_DIRECTORY, directoryStr);
        operatorProperties.put(OperatorBuilderUtils.SCHEMA, schemaJsonJSONObject.toString());
        
        DictionaryMatcherSourceOperator sourceOperator = DictionarySourceBuilder.buildSourceOperator(operatorProperties);
    }

}
