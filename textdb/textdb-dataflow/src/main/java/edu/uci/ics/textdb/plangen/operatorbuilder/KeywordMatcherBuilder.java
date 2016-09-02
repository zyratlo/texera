package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;
import edu.uci.ics.textdb.plangen.PlanGenUtils;

/**
 * KeywordMatcherBuilder is an OperatorBuilder that builds a KeywordMatcher.
 * 
 * Besides some commonly used properties (properties for attribute list, limit, offset), 
 * KeywordMatcherBuilder currently needs the following properties:
 * 
 *   keyword (required)
 *   matchingType (required)
 * 
 * @author Zuozhi Wang
 *
 */
public class KeywordMatcherBuilder {

    public static final String KEYWORD = "keyword";
    public static final String MATCHING_TYPE = "matchingType";

    /**
     * Builds a KeywordMatcher according to
     */
    public static KeywordMatcher buildOperator(Map<String, String> operatorProperties) throws PlanGenException, DataFlowException {
        String keyword = OperatorBuilderUtils.getRequiredProperty(KEYWORD, operatorProperties);
        String matchingTypeStr = OperatorBuilderUtils.getRequiredProperty(MATCHING_TYPE, operatorProperties);

        // check if keyword is empty
        PlanGenUtils.planGenAssert(!keyword.trim().isEmpty(), "keyword is empty");

        // generate attribute list
        List<Attribute> attributeList = OperatorBuilderUtils.constructAttributeList(operatorProperties);

        // generate matching type
        PlanGenUtils.planGenAssert(isValidKeywordMatchingType(matchingTypeStr), "matching type is not valid");
        KeywordMatchingType matchingType = KeywordMatchingType.valueOf(matchingTypeStr.toUpperCase());

        // build KeywordMatcher
        KeywordPredicate keywordPredicate = new KeywordPredicate(keyword, attributeList,
                DataConstants.getStandardAnalyzer(), matchingType);
        KeywordMatcher keywordMatcher = new KeywordMatcher(keywordPredicate);

        // set limit and offset
        Integer limitInt = OperatorBuilderUtils.findLimit(operatorProperties);
        if (limitInt != null) {
            keywordMatcher.setLimit(limitInt);
        }
        Integer offsetInt = OperatorBuilderUtils.findOffset(operatorProperties);
        if (offsetInt != null) {
            keywordMatcher.setOffset(offsetInt);
        }

        return keywordMatcher;
    }

    private static boolean isValidKeywordMatchingType(String matchingTypeStr) {
        return Stream.of(KeywordMatchingType.values()).map(KeywordMatchingType::name)
                .anyMatch(name -> name.toUpperCase().equals(matchingTypeStr.toUpperCase()));
    }

}
