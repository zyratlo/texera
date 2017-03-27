package edu.uci.ics.textdb.exp.keywordmatcher;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;


/**
 * @author Zuozhi Wang
 * @author prakul
 *
 * This class handles creation of predicate for querying using Keyword Matcher
 */
public class KeywordPredicate extends PredicateBase {

    /*
     * query refers to string of keywords to search for. For Ex. New york if
     * searched in TextField, we would consider both tokens New and York; if
     * searched in String field we search for Exact string.
     */
    private final String query;
    
    private final List<String> attributeNames;
    private final String luceneAnalyzerString;
    private final KeywordMatchingType matchingType;
    private final Integer limit;
    private final Integer offset;
    
    /**
     * Construct a KeywordPredicate with limit and offset set to default values.
     * 
     * @param query
     * @param attributeNames
     * @param luceneAnalyzerString
     * @param matchingType
     */
    public KeywordPredicate(
            String query,
            List<String> attributeNames,
            String luceneAnalyzerString, 
            KeywordMatchingType matchingType) {
        this(query, attributeNames, luceneAnalyzerString, matchingType, null, null);
    }
    
    /**
     * Construct a KeywordPredicate.
     * 
     * @param query, the keyword query
     * @param attributeNames, a list of attribute names to perform keyword search on
     * @param luceneAnalyzerString, a string indicating the lucene analyzer to be used. 
     *   This field is optional, passing null will set it to default value "standard"
     * @param matchingType, an Enum indicating the matching type (see KeywordMatchingType)
     * @param limit, optional, passing null will set it to default value Integer.MAX_VALUE
     * @param offset, optional, passing null will set it to default value 0
     */
    @JsonCreator
    public KeywordPredicate(
            @JsonProperty(value=PropertyNameConstants.KEYWORD_QUERY, required=true)
            String query,
            @JsonProperty(value=PropertyNameConstants.ATTRIBUTE_NAMES, required=true)
            List<String> attributeNames,
            @JsonProperty(value=PropertyNameConstants.LUCENE_ANALYZER_STRING, required=false)
            String luceneAnalyzerString, 
            @JsonProperty(value=PropertyNameConstants.KEYWORD_MATCHING_TYPE, required=true)
            KeywordMatchingType matchingType,
            @JsonProperty(value=PropertyNameConstants.LIMIT, required = false)
            Integer limit,
            @JsonProperty(value=PropertyNameConstants.OFFSET, required=false)
            Integer offset) {
        
        this.query = query;
        this.attributeNames = attributeNames;
        if (luceneAnalyzerString == null) {
            this.luceneAnalyzerString = LuceneAnalyzerConstants.standardAnalyzerString();
        } else {
            this.luceneAnalyzerString = luceneAnalyzerString;
        }
        this.matchingType = matchingType;
        
        if (limit == null) {
            this.limit = Integer.MAX_VALUE;
        } else if (limit < 0) {
            this.limit = Integer.MAX_VALUE;
            // TODO: throw exception if limit < 0
        } else {
            this.limit = limit;
        }
        
        if (offset == null) {
            this.offset = 0;
        } else if (offset < 0) {
            this.offset = 0;
            // TODO: throw exception if offset < 0
        } else {
            this.offset = offset;
        }
        
    }

    @JsonProperty(PropertyNameConstants.KEYWORD_QUERY)
    public String getQuery() {
        return query;
    }

    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAMES)
    public List<String> getAttributeNames() {
        return attributeNames;
    }

    @JsonProperty(PropertyNameConstants.LUCENE_ANALYZER_STRING)
    public String getLuceneAnalyzerString() {
        return luceneAnalyzerString;
    }

    @JsonProperty(PropertyNameConstants.KEYWORD_MATCHING_TYPE)
    public KeywordMatchingType getMatchingType() {
        return matchingType;
    }
    
    @JsonProperty(PropertyNameConstants.LIMIT)
    public Integer getLimit() {
        return this.limit;
    }
    
    @JsonProperty(PropertyNameConstants.OFFSET)
    public Integer getOffset() {
        return this.offset;
    }

}
