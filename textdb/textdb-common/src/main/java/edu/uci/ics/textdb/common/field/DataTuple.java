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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fields == null) ? 0 : fields.hashCode());
		result = prime * result + ((schema == null) ? 0 : schema.hashCode());
		return result;
	}
    
    
    

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataTuple other = (DataTuple) obj;
		if (fields == null) {
			if (other.fields != null)
				return false;
		} else if (!fields.equals(other.fields))
			return false;
		if (schema == null) {
			if (other.schema != null)
				return false;
		} else if (!schema.equals(other.schema))
			return false;
		return true;
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
