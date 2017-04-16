package edu.uci.ics.textdb.exp.common;

/**
 * PropertyNameConstants defines the key names 
 *   in the JSON representation of each operator.
 * 
 * @author Zuozhi Wang
 *
 */
public class PropertyNameConstants {
    
    private PropertyNameConstants() {};
    
    // common property names
    public static final String ATTRIBUTE_NAMES = "attributes";
    public static final String ATTRIBUTE_NAME = "attribute";
    public static final String RESULT_ATTRIBUTE_NAME = "result_attribute";
    public static final String LUCENE_ANALYZER_STRING = "lucene_analzer";
    public static final String SPAN_LIST_NAME = "span_list_name";
    public static final String TABLE_NAME = "data_source";
    public static final String FILE_PATH = "file_path";
    public static final String LIMIT = "limit";
    public static final String OFFSET = "offset";
    
    // related to keyword matcher
    public static final String KEYWORD_QUERY = "query";
    public static final String KEYWORD_MATCHING_TYPE = "matching_type";
    
    // related to dictionary matcher
    public static final String DICTIONARY = "dictionary";
    public static final String DICTIONARY_ENTRIES = "dictionaryEntries";
    
    // related to regex matcher
    public static final String REGEX = "regex";
    public static final String REGEX_IGNORE_CASE = "regexIgnoreCase";
    public static final String REGEX_USE_INDEX = "regexUseIndex";
    
    // related to fuzzy token matcher
    public static final String FUZZY_TOKEN_QUERY = "query";
    public static final String FUZZY_TOKEN_THRESHOLD_RATIO = "thresholdRatio";
    
    // related to nlp extractor
    public static final String NLP_ENTITY_TYPE = "nlpEntityType";
    
    // related to regex splitter
    public static final String SPLIT_ATTRIBUTE = "splitAttribute";
    public static final String SPLIT_TYPE = "splitType";
    public static final String SPLIT_REGEX = "splitRegex";

    // related to sampler
    public static final String SAMPLE_SIZE = "sample_size";
    public static final String SAMPLE_TYPE = "sample_type";
    
    // related to file source
    public static final String FILE_MAX_DEPTH = "max_depth";
    public static final String FILE_RECURSIVE = "recursive";
    public static final String FILE_ALLOWED_EXTENSIONS = "allowed_extensions";
    
    // related to join
    public static final String INNER_ATTRIBUTE_NAME = "innerAttribute";
    public static final String OUTER_ATTRIBUTE_NAME = "outerAttribute";
    public static final String SPAN_DISTANCE = "spanDistance";
    public static final String JOIN_SIMILARITY_THRESHOLD = "similarityThreshold";

    
}
