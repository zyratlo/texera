package edu.uci.ics.textdb.common.field;

import edu.uci.ics.textdb.api.common.IField;

import java.util.List;

/**
 * Created by kishorenarendran on 25/04/16.
 */
public class ListField<T> implements IField {
    private final List<T> value;

    public ListField(List<T> value) {
        this.value = value;
    }

    @Override
    public List<T> getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public String toString() {
        String getStringResult = new String();
        for(T val: value) {
            getStringResult = getStringResult.concat(val.toString().concat(" "));
        }
        getStringResult = getStringResult.trim();
        return "ListField [value=" + getStringResult + "]";
    }
}
