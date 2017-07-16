package edu.uci.ics.textdb.api.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.constants.JsonConstants;

public class Attribute {
    private final String attributeName;
    private final AttributeType attributeType;

    @JsonCreator
    public Attribute(
            @JsonProperty(value = JsonConstants.ATTRIBUTE_NAME, required = true)
            String attributeName, 
            @JsonProperty(value = JsonConstants.ATTRIBUTE_TYPE, required = true)
            AttributeType attributeType) {     
        this.attributeName = attributeName;
        this.attributeType = attributeType;
    }
    
    @JsonProperty(value = JsonConstants.ATTRIBUTE_NAME)
    public String getAttributeName() {
        return attributeName;
    }

    @JsonProperty(value = JsonConstants.ATTRIBUTE_TYPE)
    public AttributeType getAttributeType() {
        return attributeType;
    }

    @Override
    public String toString() {
        return "Attribute [attributeName=" + attributeName + ", attributeType=" + attributeType + "]";
    }
    
    @Override
    public boolean equals(Object toCompare) {
        if (this == toCompare) {
            return true;
        }
        if (toCompare == null) {
            return false;
        }
        if (this.getClass() != toCompare.getClass()) {
            return false;
        }
        
        Attribute that = (Attribute) toCompare;
        
        if (this.attributeName == null) {
            return that.attributeName == null;
        }
        if (this.attributeType == null) {
            return that.attributeType == null;
        }
        
        return this.attributeName.equals(that.attributeName) && this.attributeType.equals(that.attributeType);
    }
    
    @Override
    public int hashCode() {
        return this.attributeName.hashCode() + this.attributeType.toString().hashCode();
    }
}
