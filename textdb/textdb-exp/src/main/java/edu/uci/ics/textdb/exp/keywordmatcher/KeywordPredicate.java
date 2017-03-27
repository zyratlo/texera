package edu.uci.ics.textdb.exp.keywordmatcher;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.dataflow.IOperator;
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
    
    private final List<String> attributes;
    private final String luceneAnalyzer;
    private final KeywordMatchingType matchingType;
    private final String spanListName;
    
    @JsonCreator
    public KeywordPredicate(
            @JsonProperty(value=PropertyNameConstants.KEYWORD_QUERY, required=true)
            String query,
            @JsonProperty(value=PropertyNameConstants.ATTRIBUTES, required=true)
            List<String> attributes,
            @JsonProperty(value=PropertyNameConstants.LUCENE_ANALYZER, required=false)
            String luceneAnalyzer, 
            @JsonProperty(value=PropertyNameConstants.KEYWORD_MATCHING_TYPE, required=true)
            KeywordMatchingType matchingType,
            @JsonProperty(value=PropertyNameConstants.SPAN_LIST_NAME, required=false)
            String spanListName) {
        
        this.query = query;
        this.attributes = attributes;
        if (luceneAnalyzer == null) {
            this.luceneAnalyzer = LuceneAnalyzerConstants.standardAnalyzerString();
        } else {
            this.luceneAnalyzer = luceneAnalyzer;
        }
        this.matchingType = matchingType;
        this.spanListName = spanListName;
    }

    @JsonProperty(PropertyNameConstants.KEYWORD_QUERY)
    public String getQuery() {
        return query;
    }

    @JsonProperty(PropertyNameConstants.ATTRIBUTES)
    public List<String> getAttributes() {
        return attributes;
    }

    @JsonProperty(PropertyNameConstants.LUCENE_ANALYZER)
    public String getLuceneAnalyzer() {
        return luceneAnalyzer;
    }

    @JsonProperty(PropertyNameConstants.KEYWORD_MATCHING_TYPE)
    public KeywordMatchingType getMatchingType() {
        return matchingType;
    }
    
    @JsonProperty(PropertyNameConstants.SPAN_LIST_NAME)
    public String getSpanListName() {
        return this.spanListName;
    }

}
