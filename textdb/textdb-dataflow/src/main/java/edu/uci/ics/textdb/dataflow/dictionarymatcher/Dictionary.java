package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.util.List;

import edu.uci.ics.textdb.api.common.IDictionary;

/**
 * @author Sudeep [inkudo]
 *
 */
public class Dictionary implements IDictionary {

    private int cursor = -1;
    private List<String> dict;

    public Dictionary(List<String> dict) {
        this.dict = dict;
        cursor = 0;
    }

    @Override
    /**
     * Gets next value from the dictionary
     */
    public String getNextDictValue() {
        if (cursor >= dict.size()) {
            cursor = 0;
            return null;
        }
        String dictval = dict.get(cursor++);
        return dictval;
    }

}
