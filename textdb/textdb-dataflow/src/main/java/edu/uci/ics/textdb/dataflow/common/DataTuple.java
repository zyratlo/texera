/**
 * 
 */
package edu.uci.ics.textdb.dataflow.common;

import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;

/**
 * @author sandeepreddy602
 *
 */
public class DataTuple implements ITuple{
    private final List<String> schema;
    private final List<IField> fields;

    public DataTuple(List<String> schema, IField... fields) {
        this.schema = schema;
        this.fields = Arrays.asList(fields);
    }

    @Override
    public IField getField(int index) {
        return fields.get(index);
    }

    @Override
    public IField getField(String fieldName) {
        int index = schema.indexOf(fieldName);
        if (index < 0) {
            return null;
        }
        return getField(index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataTuple that = (DataTuple) o;

        if (schema != that.schema) return false;
        return fields.equals(that.fields);

    }

    @Override
    public String toString() {
        return "DataTuple [schema=" + schema + ", fields=" + fields + "]";
    }
}
