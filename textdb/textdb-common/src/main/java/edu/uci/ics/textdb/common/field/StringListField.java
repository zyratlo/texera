package edu.uci.ics.textdb.common.field;

import edu.uci.ics.textdb.api.common.IField;

import java.util.List;

/**
 * Created by kishorenarendran on 25/04/16.
 */
public class StringListField implements IField {
    private final List<String> value;

    public StringListField(List<String> value) {
        this.value = value;
    }

    @Override
    public List<String> getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public String toString() {
        String getStringResult = new String();
        for(String val: value) {
            getStringResult = getStringResult.concat(val.concat(" "));
        }
        getStringResult = getStringResult.trim();
        return "StringListField [value=" + getStringResult + "]";
    }
}
