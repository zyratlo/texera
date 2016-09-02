package edu.uci.ics.textdb.dataflow.plangen.operatorbuilder;

import java.util.HashMap;

import org.junit.Test;

import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;
import edu.uci.ics.textdb.plangen.operatorbuilder.KeywordMatcherBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.OperatorBuilderUtils;

public class KeywordMatcherBuilderTest {
    
    
    @Test
    public void testKeywordMatcherBuilder1() throws PlanGenException, DataFlowException {
        HashMap<String, String> keywordProperties = new HashMap<>();
        keywordProperties.put(KeywordMatcherBuilder.KEYWORD, "zika");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        keywordProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "TEXT");
        keywordProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "conjunction_indexbased");
        
        KeywordMatcher keywordMatcher = KeywordMatcherBuilder.buildOperator(keywordProperties);
        
        
    }

}
