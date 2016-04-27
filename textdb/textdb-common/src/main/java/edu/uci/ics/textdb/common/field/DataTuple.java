package edu.uci.ics.textdb.common.field;

import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;

/**
 * @author sandeepreddy602
 *
 */
public class DataTuple implements ITuple {
    private final Schema schema;
    private final List<IField> fields;

    public DataTuple(Schema schema, IField... fields) {
        this.schema = schema;
        this.fields = Arrays.asList(fields);
    }

    @Override
    public IField getField(int index) {
        return fields.get(index);
    }

    @Override
    public IField getField(String fieldName) {
        int index = schema.getIndex(fieldName);
        return getField(index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DataTuple that = (DataTuple) o;

        if (schema != that.schema)
            return false;
        return fields.equals(that.fields);

    }

    @Override
    public String toString() {
        return "DataTuple [schema=" + schema + ", fields=" + fields + "]";
    }

    @Override
    public List<IField> getFields() {
        return fields;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

}
