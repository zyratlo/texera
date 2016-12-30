package edu.uci.ics.textdb.textql.statements.predicates;

import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;


/**
 * Object representation of a "KEYWORDEXTRACT(...)" predicate inside a { @code SelectExtractStatement }.
 * 
 * @author Flavio Bayer
 *
 */
public class KeywordExtractPredicate extends ExtractPredicate {
    
    /**
     * The { @link List } of fields on which the keyword search should be performed.
     */
    public List<String> matchingFields;
    
    /**
     * The keyword(s) used for a keyword search.
     */
    public String keywords;
    
    /**
     * The type of matching to be performed during the keyword search.
     */ 
    public String matchingType;
    
    
    /**
     * Create a { @code KeywordExtractPredicate } with all the parameters set to { @code null }.
     * @param id The id of this statement.
     */
    public KeywordExtractPredicate() {
        this(null, null, null);
    }

    /**
     * Create a { @code KeywordExtractPredicate } with the given parameters.
     * @param matchingFields List of fields to extract information from.
     * @param keywords The keywords to look for during extraction.
     * @param matchingType The string representation of the { @code KeywordMatchingType } used for extraction.
     */
    public KeywordExtractPredicate(List<String> matchingFields, String keywords, String matchingType) {
        this.matchingFields = matchingFields;
        this.keywords = keywords;
        this.matchingType = matchingType;
    }
        

    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != getClass()) { return false; }
        KeywordExtractPredicate keywordExtractPredicate = (KeywordExtractPredicate) other;
        return new EqualsBuilder()
                .appendSuper(super.equals(other))
                .append(matchingFields, keywordExtractPredicate.matchingFields)
                .append(keywords, keywordExtractPredicate.keywords)
                .append(matchingType, keywordExtractPredicate.matchingType)
                .isEquals();
    }
    
}