/**
 * 
 */
package edu.uci.ics.textdb.common.constants;

/**
 * @author sandeepreddy602
 * @author Zuozhi Wang (zuozhi)
 *
 */
public class DataConstants {
    public static final String INDEX_DIR = "../index";
    public static final String SCAN_QUERY = "*:*";
    public static final int MAX_RESULTS = 100;
    
    /**
     * KeywordMatchingType: the type of keyword matching to perform. <br>
     * Currently we have 3 types of keyword matching: <br>
     * 
     * SUBSTRING_SCANBASED: <br>
     * Performs simple substring matching of the query.
     * Source tuples are provided by ScanSourceOperator. <br>
     * 
     * CONJUNCTION_INDEXBASED: <br>
     * Performs search of conjunction of query tokens. 
     * The query is tokenized into tokens, with each token treated as a separate keyword. 
     * The order of tokens doesn't matter on the source tuple. <br>
     * 
     * For example: <br>
     * query "book appointment" <br>
     * matches: "book appointment with the doctor" <br>
     * also matches: "an appointment to pick up book" <br>
     * <br>
     * 
     * 
     * PHRASE_INDEXBASED: <br>
     * Performs phrase search of the query.
     * The query is tokenized into tokens, with stopwords treated as placeholders.
     * The order of tokens matters on the source tuple. A stopword matches an arbitary token. <br>
     * 
     * For example: <br>
     * query "book appointment" <br>
     * matches: "book appointment with the doctor" <br>
     * doesn't match: "an appointment to pick up book" <br>
     * 
     * Example of stopword as placeholders: <br>
     * query "nice a a person":
     * matches "nice and beautiful person" <br>
     * matches "nice gentle honest person" <br>
     * doesn't match "nice person" <br>
     * doesn't match "nice gentle person" <br>
     * <br>
     * 
     */
    public static enum KeywordMatchingType {
        SUBSTRING_SCANBASED, 
        
        CONJUNCTION_INDEXBASED, 
        
        PHRASE_INDEXBASED
    };
    
}
