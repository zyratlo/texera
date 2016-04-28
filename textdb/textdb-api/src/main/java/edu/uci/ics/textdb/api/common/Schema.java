package edu.uci.ics.textdb.api.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Schema {
    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
		result = prime * result + ((fieldNameVsIndex == null) ? 0 : fieldNameVsIndex.hashCode());
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
		Schema other = (Schema) obj;
		if (attributes == null) {
			if (other.attributes != null)
				return false;
		} else if (!attributes.equals(other.attributes))
			return false;
		if (fieldNameVsIndex == null) {
			if (other.fieldNameVsIndex != null)
				return false;
		} else if (!fieldNameVsIndex.equals(other.fieldNameVsIndex))
			return false;
		return true;
	}

	private List<Attribute> attributes;
    private Map<String, Integer> fieldNameVsIndex;
    
    public Schema(Attribute... attributes){
        this.attributes = Arrays.asList(attributes);
        populateFieldNameVsIndexMap();
    }
    
    private void populateFieldNameVsIndexMap() {
        fieldNameVsIndex = new HashMap<String, Integer>();
        for (int count = 0; count < attributes.size(); count++) {
            String fieldName = attributes.get(count).getFieldName();
            fieldNameVsIndex.put(fieldName.toLowerCase(), count);
        }
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }
    
    public int getIndex(String fieldName){
        return fieldNameVsIndex.get(fieldName.toLowerCase());
    }
}
