package edu.uci.ics.textdb.api.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Schema {
    private List<Attribute> attributes;
    private Map<String, Integer> fieldNameVsIndex;
    
    public Schema(List<Attribute> attributes){
        this.attributes = attributes;
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
