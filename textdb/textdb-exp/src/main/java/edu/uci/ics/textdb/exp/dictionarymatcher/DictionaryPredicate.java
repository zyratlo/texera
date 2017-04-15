package edu.uci.ics.textdb.exp.dictionarymatcher;

import java.util.List;

import edu.uci.ics.textdb.api.dataflow.IPredicate;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordMatchingType;

public class DictionaryPredicate implements IPredicate {

    private Dictionary dictionary;
    private String luceneAnalyzerStr;
    private List<String> attributeNames;
    private KeywordMatchingType keywordMatchingType;

    /*
     * dictionary refers to list of phrases to search for. For Ex. New York if
     * searched in TextField, we would consider both tokens New and York; if
     * searched in String field we search for Exact string.
     */

    public DictionaryPredicate(Dictionary dictionary, List<String> attributeNames, String luceneAnalyzerStr,
            KeywordMatchingType keywordMatchingType) {

        this.dictionary = dictionary;
        this.luceneAnalyzerStr = luceneAnalyzerStr;
        this.attributeNames = attributeNames;
        this.keywordMatchingType = keywordMatchingType;
    }

    public KeywordMatchingType getKeywordMatchingType() {
        return keywordMatchingType;
    }

    /**
     * Reset the Dictionary Cursor to the beginning.
     */
    public void resetDictCursor() {
        dictionary.resetCursor();
    }

    public String getNextDictionaryEntry() {
        return dictionary.getNextValue();
    }

    public List<String> getAttributeNames() {
        return attributeNames;
    }

    public String getAnalyzerString() {
        return luceneAnalyzerStr;
    }

}
