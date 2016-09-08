package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.dataflow.common.Dictionary;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.DictionaryMatcherSourceOperator;
import junit.framework.Assert;

public class DictionarySourceBuilderTest {
    
    @Test
    public void testDictionarySourceBuilder1() throws Exception {
        String directoryStr = "./index";
        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");
        
        System.out.println(schemaJsonJSONObject.toString());
        
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
        Dictionary dictionary = new Dictionary(dictionaryList);
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

        
        String dictEntry = null;
        ArrayList<String> actualDict = new ArrayList<>();
        while ((dictEntry = sourceOperator.getPredicate().getNextDictionaryEntry()) != null) {
            actualDict.add(dictEntry);
        }
        
        // compare dict
        Assert.assertEquals(dictionaryList, actualDict);
        // compare dataStore directory
        Assert.assertEquals(directoryStr, sourceOperator.getDataStore().getDataDirectory());
        // compare dataStore schema
        Assert.assertEquals(
                schemaAttrs.stream().collect(Collectors.toList()).toString(), 
                sourceOperator.getDataStore().getSchema().getAttributes().stream().collect(Collectors.toList()).toString());
        // compare dictMatcher attribute list
        Assert.assertEquals(dictAttrs.toString(), sourceOperator.getPredicate().getAttributeList().toString());

    }
    
    

}
