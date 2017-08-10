package edu.uci.ics.textdb.exp.dictionarymatcher;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.exp.common.PropertyNameConstants;
import edu.uci.ics.textdb.exp.utils.DataflowUtils;

/**
 * @author Sudeep [inkudo]
 */
public class Dictionary {

    private final LinkedHashSet<String> dictionaryEntries;
    private Iterator<String> dictionaryIterator;
    private ArrayList<Set<String>> dictionaryTokenSetList;
    private ArrayList<List<String>> dictionaryTokenListWithStopwords;
    private ArrayList<List<String>> dictionaryTokenList;

    /**
     * Create a dictionary using a collection of entries.
     *
     * @param dictionaryEntries, a collection of dictionary entries
     */
    @JsonCreator
    public Dictionary(
            @JsonProperty(value = PropertyNameConstants.DICTIONARY_ENTRIES, required = true)
                    Collection<String> dictionaryEntries) {
        // Using LinkedHashSet so that getNextValue() returns the words in order.
        this.dictionaryEntries = new LinkedHashSet<>();
        for (String entry : dictionaryEntries) {
            if (!entry.trim().equals("")) {
                this.dictionaryEntries.add(entry.trim());
            }
        }

        this.dictionaryIterator = this.dictionaryEntries.iterator();
        this.dictionaryTokenList = null;
        this.dictionaryTokenListWithStopwords = null;
        this.dictionaryTokenSetList = null;
    }

    @JsonProperty(value = PropertyNameConstants.DICTIONARY_ENTRIES)
    public Collection<String> getDictionaryEntries() {
        return new ArrayList<>(this.dictionaryEntries);
    }

    /**
     * Gets next dictionary entry from the dictionary
     */
    @JsonIgnore
    public String getNextEntry() {
        if (dictionaryIterator.hasNext()) {
            return dictionaryIterator.next();
        }
        return null;
    }

    public boolean isEmpty() {
        return (dictionaryEntries == null || dictionaryEntries.isEmpty());
    }

    /***
     * To generate set of tokens for each dictionary entry with removal of duplicate tokens.
     * Used by Conjunction matching type.
     * For example dictionaryEntries: {"San Jose", "Newport Beach", "lin lin panda"}
     * Set of tokens for "San Jose": {san, jose}
     * Set of tokens for "Newport Beach": {newport, beach}
     * Set of tokens for "lin lin panda": {lin, panda}
     * @param luceneAnalyzerStr
     */

    public void setDictionaryTokenSetList(String luceneAnalyzerStr) {
        this.dictionaryTokenSetList = new ArrayList<>();
        resetCursor();
        while (dictionaryIterator.hasNext()) {
            dictionaryTokenSetList.add(new HashSet<>(DataflowUtils.tokenizeQuery(luceneAnalyzerStr, dictionaryIterator.next())));
        }
    }

    /**
     * To generate List of tokens for each dictionary entry without removal of duplicate tokens:
     * and List of tokens for each dictionary entry without removal of stopwords.
     * Used by Phrase matching type.
     * Example: {"lin lin panda"}: List of tokens will be {lin, lin, panda}.
     * Example: {"to be or not to be"}: tokenize with stopwords: {to, be, or, not, to, be}.
     *
     * @param luceneAnalyzerStr
     */

    public void setDictionaryTokenListWithStopwords(String luceneAnalyzerStr) {
        this.dictionaryTokenListWithStopwords = new ArrayList<>();
        this.dictionaryTokenList = new ArrayList<>();
        resetCursor();
        while (dictionaryIterator.hasNext()) {
            String dictionaryEntry = dictionaryIterator.next();
            dictionaryTokenList.add(DataflowUtils.tokenizeQuery(luceneAnalyzerStr, dictionaryEntry));
            dictionaryTokenListWithStopwords.add(DataflowUtils.tokenizeQueryWithStopwords(dictionaryEntry));

        }
    }
    @JsonIgnore
    public ArrayList<Set<String>> getDictionaryTokenSetList() {
        return this.dictionaryTokenSetList;
    }

    @JsonIgnore
    public ArrayList<List<String>> getDictionaryTokenListWithStopwords() {
        return this.dictionaryTokenListWithStopwords;
    }

    @JsonIgnore
    public ArrayList<List<String>> getDictionaryTokenList() {
        return this.dictionaryTokenList;
    }

    /**
     * Reset the cursor to the start of the dictionary.
     */
    @JsonIgnore
    public void resetCursor() {
        dictionaryIterator = dictionaryEntries.iterator();
    }

}
