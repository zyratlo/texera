package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import junit.framework.Assert;

/**
 * Tests cases for RegexMatcherBuilder
 * @author Zuozhi Wang
 *
 */
public class RegexMatcherBuilderTest {

    /*
     * test RegexMatcherBuilder with the following properties:
     *   regex: \b(woman)|(man)|(patient)\b
     *   attribute list: {content, TEXT}
     *   limit: default (Integer.MAX_VALUE)
     *   offset: default (0)
     * 
     */
    @Test
    public void testRegexMatcherBuilder1() throws PlanGenException, DataFlowException, IOException {
        String regex = "\\b(woman)|(man)|(patient)\\b";
        
        HashMap<String, String> regexProperties = new HashMap<>();
        regexProperties.put(RegexMatcherBuilder.REGEX, regex);
        regexProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        regexProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "TEXT");
        
        RegexMatcher regexMatcher = RegexMatcherBuilder.buildRegexMatcher(regexProperties);
        
        Assert.assertEquals(regex, regexMatcher.getPredicate().getRegex());
        List<Attribute> regexAttrList = Arrays.asList(
                new Attribute("content", FieldType.TEXT));
        Assert.assertEquals(regexAttrList.toString(), regexMatcher.getPredicate().getAttributeList().toString());
        Assert.assertEquals(Integer.MAX_VALUE, regexMatcher.getLimit());
        Assert.assertEquals(0, regexMatcher.getOffset());    
    }
    
    /*
     * test RegexMatcherBuilder with the following properties:
     *   regex: (19|20)\d\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])
     *   attribute list: {date, DATE}, {amount, INTEGER}, {description, TEXT}
     *   limit: 100
     *   offset: 11
     * 
     */
    @Test
    public void testRegexMatcherBuilder2() throws PlanGenException, DataFlowException, IOException {
        String regex = "(19|20)\\d\\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])";
        
        HashMap<String, String> regexProperties = new HashMap<>();
        regexProperties.put(RegexMatcherBuilder.REGEX, regex);
        regexProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "date, amount, description");
        regexProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "DATE, INTEGER, TEXT");
        regexProperties.put(OperatorBuilderUtils.LIMIT, "100");
        regexProperties.put(OperatorBuilderUtils.OFFSET, "11");
        
        RegexMatcher regexMatcher = RegexMatcherBuilder.buildRegexMatcher(regexProperties);
        
        Assert.assertEquals(regex, regexMatcher.getPredicate().getRegex());
        List<Attribute> regexAttrList = Arrays.asList(
                new Attribute("date", FieldType.DATE), 
                new Attribute("amount", FieldType.INTEGER), 
                new Attribute("description", FieldType.TEXT));
        Assert.assertEquals(regexAttrList.toString(), regexMatcher.getPredicate().getAttributeList().toString());
        Assert.assertEquals(100, regexMatcher.getLimit());
        Assert.assertEquals(11, regexMatcher.getOffset());    
    }
    
    /*
     * test invalid RegexMatcherBuilder with missing regex properties
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidRegexMatcherBuilder1() throws PlanGenException, DataFlowException, IOException {
        HashMap<String, String> regexProperties = new HashMap<>();
        regexProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "date, amount, description");
        regexProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "DATE, INTEGER, TEXT");
        regexProperties.put(OperatorBuilderUtils.LIMIT, "100");
        regexProperties.put(OperatorBuilderUtils.OFFSET, "11");
        
        RegexMatcherBuilder.buildRegexMatcher(regexProperties); 
    }
    
    /*
     * test invalid RegexMatcherBuilder with empty regex
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidRegexMatcherBuilder2() throws PlanGenException, DataFlowException, IOException {
        HashMap<String, String> regexProperties = new HashMap<>();
        regexProperties.put(RegexMatcherBuilder.REGEX, "");
        regexProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "date, amount, description");
        regexProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "DATE, INTEGER, TEXT");
        regexProperties.put(OperatorBuilderUtils.LIMIT, "100");
        regexProperties.put(OperatorBuilderUtils.OFFSET, "11");
        
        RegexMatcherBuilder.buildRegexMatcher(regexProperties); 
    }
}
