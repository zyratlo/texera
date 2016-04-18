package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.util.List;

import edu.uci.ics.textdb.api.common.IDictionary;

/**
 * @author Sudeep [inkudo]
 *
 */
public class Dictionary implements IDictionary {

    private int cursor = -1;
    private List<String> stringList;

    public Dictionary(List<String> dict) {
        this.stringList = dict;
        cursor = 0;
    }

    /**
     * Gets next value from the dictionary
     */
    @Override
    public String getNextValue() {
        if (cursor >= stringList.size()) {
            return null;
        }
        String dictval = stringList.get(cursor++);
        return dictval;
    }

}
