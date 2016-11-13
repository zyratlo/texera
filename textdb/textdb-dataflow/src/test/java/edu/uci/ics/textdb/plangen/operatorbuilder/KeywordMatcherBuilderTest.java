package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;
import junit.framework.Assert;

/**
 * Tests cases for KeywordMatcherBuilder
 * @author Zuozhi Wang
 *
 */
public class KeywordMatcherBuilderTest {
    
    /*
     * test KeywordMatcherBuilder with the following properties:
     *   keyword: zika
     *   attribute list: {content, TEXT}
     *   matching type: conjunction_indexbased
     *   limit: default (Integer.MAX_VALUE)
     *   offset: default (0)
     * 
     */
    @Test
    public void testKeywordMatcherBuilder1() throws PlanGenException, DataFlowException {
        HashMap<String, String> keywordProperties = new HashMap<>();
        keywordProperties.put(KeywordMatcherBuilder.KEYWORD, "zika");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "TEXT");
        keywordProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "conjunction_indexbased");
        
        KeywordMatcher keywordZika = KeywordMatcherBuilder.buildKeywordMatcher(keywordProperties);
        
        Assert.assertEquals("zika", keywordZika.getPredicate().getQuery());
        List<Attribute> zikaAttrList = Arrays.asList(
                new Attribute("content", FieldType.TEXT));
        Assert.assertEquals(zikaAttrList.stream().map(attr -> attr.getFieldName()).collect(Collectors.toList()).toString(), 
                keywordZika.getPredicate().getAttributeNames().toString());
        Assert.assertEquals(KeywordMatchingType.CONJUNCTION_INDEXBASED, keywordZika.getPredicate().getOperatorType());
        Assert.assertEquals(Integer.MAX_VALUE, keywordZika.getLimit());
        Assert.assertEquals(0, keywordZika.getOffset());
        
    }
    
    /*
     * test KeywordMatcherBuilder with the following properties:
     *   keyword: Irvine
     *   attribute list: {city, STRING}, {location, STRING}, {content, TEXT}
     *   matching type: substring_scanbased
     *   limit: 10
     *   offset: 2
     * 
     */
    @Test
    public void testKeywordMatcherBuilder2() throws PlanGenException, DataFlowException {
        HashMap<String, String> keywordProperties = new HashMap<>();
        keywordProperties.put(KeywordMatcherBuilder.KEYWORD, "Irvine");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
        keywordProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "SUBSTRING_SCANBASED");
        keywordProperties.put(OperatorBuilderUtils.LIMIT, "10");
        keywordProperties.put(OperatorBuilderUtils.OFFSET, "2");
        
        KeywordMatcher keywordIrvine = KeywordMatcherBuilder.buildKeywordMatcher(keywordProperties);
               
        Assert.assertEquals("Irvine", keywordIrvine.getPredicate().getQuery());
        List<Attribute> irvineAttrList = Arrays.asList(
                new Attribute("city", FieldType.STRING),
                new Attribute("location", FieldType.STRING),
                new Attribute("content", FieldType.TEXT));
        Assert.assertEquals(irvineAttrList.stream().map(attr -> attr.getFieldName()).collect(Collectors.toList()).toString(), 
                keywordIrvine.getPredicate().getAttributeNames().toString());
        Assert.assertEquals(KeywordMatchingType.SUBSTRING_SCANBASED, keywordIrvine.getPredicate().getOperatorType());
        Assert.assertEquals(10, keywordIrvine.getLimit());
        Assert.assertEquals(2, keywordIrvine.getOffset());     
    }
    
    /*
     * test invalid KeywordMatcherBuilder with missing keyword property
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidKeywordMatcherBuilder1() throws PlanGenException, DataFlowException {
        HashMap<String, String> keywordProperties = new HashMap<>();
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
        keywordProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "SUBSTRING_SCANBASED");
        keywordProperties.put(OperatorBuilderUtils.LIMIT, "10");
        keywordProperties.put(OperatorBuilderUtils.OFFSET, "2");
        
        KeywordMatcherBuilder.buildKeywordMatcher(keywordProperties);       
    }
    
    /*
     * test invalid KeywordMatcherBuilder with empty keyword 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidKeywordMatcherBuilder2() throws PlanGenException, DataFlowException {
        HashMap<String, String> keywordProperties = new HashMap<>();
        keywordProperties.put(KeywordMatcherBuilder.KEYWORD, "");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
        keywordProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "SUBSTRING_SCANBASED");
        keywordProperties.put(OperatorBuilderUtils.LIMIT, "10");
        keywordProperties.put(OperatorBuilderUtils.OFFSET, "2");
        
        KeywordMatcherBuilder.buildKeywordMatcher(keywordProperties);      
    }
    
    /*
     * test invalid KeywordMatcherBuilder with missing attribute names property
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidKeywordMatcherBuilder3() throws PlanGenException, DataFlowException {
        HashMap<String, String> keywordProperties = new HashMap<>();
        keywordProperties.put(KeywordMatcherBuilder.KEYWORD, "Irvine");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
        keywordProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "SUBSTRING_SCANBASED");
        keywordProperties.put(OperatorBuilderUtils.LIMIT, "10");
        keywordProperties.put(OperatorBuilderUtils.OFFSET, "2");
        
        KeywordMatcherBuilder.buildKeywordMatcher(keywordProperties);       
    }
    
    /*
     * test invalid KeywordMatcherBuilder with missing invalid attribute types property
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidKeywordMatcherBuilder4() throws PlanGenException, DataFlowException {
        HashMap<String, String> keywordProperties = new HashMap<>();
        keywordProperties.put(KeywordMatcherBuilder.KEYWORD, "Irvine");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "invalid_type, another_invalid_type, TEXT");
        keywordProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "SUBSTRING_SCANBASED");
        keywordProperties.put(OperatorBuilderUtils.LIMIT, "10");
        keywordProperties.put(OperatorBuilderUtils.OFFSET, "2");
        
        KeywordMatcherBuilder.buildKeywordMatcher(keywordProperties);       
    }
    
    /*
     * test invalid KeywordMatcherBuilder with inconsistent attribute names and types
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidKeywordMatcherBuilder5() throws PlanGenException, DataFlowException {
        HashMap<String, String> keywordProperties = new HashMap<>();
        keywordProperties.put(KeywordMatcherBuilder.KEYWORD, "Irvine");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT, TEXT, TEXT");
        keywordProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "SUBSTRING_SCANBASED");
        keywordProperties.put(OperatorBuilderUtils.LIMIT, "10");
        keywordProperties.put(OperatorBuilderUtils.OFFSET, "2");
        
        KeywordMatcherBuilder.buildKeywordMatcher(keywordProperties);  
    }
    
    /*
     * test invalid KeywordMatcherBuilder with invalid matching type
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidKeywordMatcherBuilder6() throws PlanGenException, DataFlowException {
        HashMap<String, String> keywordProperties = new HashMap<>();
        keywordProperties.put(KeywordMatcherBuilder.KEYWORD, "Irvine");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
        keywordProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "invalid_matching_type");
        keywordProperties.put(OperatorBuilderUtils.LIMIT, "10");
        keywordProperties.put(OperatorBuilderUtils.OFFSET, "2");
        
        KeywordMatcherBuilder.buildKeywordMatcher(keywordProperties);  
    }

}
