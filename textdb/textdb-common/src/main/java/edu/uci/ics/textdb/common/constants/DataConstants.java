/**
 * 
 */
package edu.uci.ics.textdb.common.constants;

/**
 * @author sandeepreddy602
 *
 */
public class DataConstants {
    public static final String INDEX_DIR = "../index";
    public static final String SCAN_QUERY = "*:*";
    public static final int MAX_RESULTS = 100;
    
    public static enum KeywordMatchingType {
        SUBSTRING_SCANBASED, CONJUNCTION_INDEXBASED, PHRASE_INDEXBASED
    };
    
}
