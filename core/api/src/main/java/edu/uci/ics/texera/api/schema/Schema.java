package edu.uci.ics.texera.api.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import static com.google.common.base.Preconditions.checkNotNull;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.JsonConstants;
import edu.uci.ics.texera.api.exception.TexeraException;

public class Schema {
    private final ImmutableList<Attribute> attributes;
    private final ImmutableMap<String, Integer> attributeIndex;

    public Schema(Attribute... attributes) {
        this(Arrays.asList(attributes));
    }
    
    @JsonCreator
    public Schema(
            @JsonProperty(value = JsonConstants.ATTRIBUTES, required = true)
            List<Attribute> attributes) {
        checkNotNull(attributes);
        this.attributes = ImmutableList.copyOf(attributes);
        HashMap<String, Integer> attributeIndexTemp = new HashMap<String, Integer>();
        for (int i = 0; i < attributes.size(); i++) {
            attributeIndexTemp.put(attributes.get(i).getName().toLowerCase(), i);
        }
        this.attributeIndex = ImmutableMap.copyOf(attributeIndexTemp);
    }

    @JsonProperty(value = JsonConstants.ATTRIBUTES)
    public List<Attribute> getAttributes() {
        return attributes;
    }
    
    @JsonIgnore
    public List<String> getAttributeNames() {
        return attributes.stream().map(attr -> attr.getName()).collect(Collectors.toList());
    }

    public Integer getIndex(String attributeName) {
	    	if (! containsAttribute(attributeName)) {
	    		throw new TexeraException(attributeName + " is not contained in the schema");
	    	}
	    return attributeIndex.get(attributeName.toLowerCase());
    }
    
    public Attribute getAttribute(String attributeName) {
        return attributes.get(getIndex(attributeName));
    }

    @JsonIgnore
    public boolean containsAttribute(String attributeName) {
        return attributeIndex.containsKey(attributeName.toLowerCase());
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
        result = prime * result + ((attributeIndex == null) ? 0 : attributeIndex.hashCode());
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
        if (attributeIndex == null) {
            if (other.attributeIndex != null)
                return false;
        } else if (!attributeIndex.equals(other.attributeIndex))
            return false;
        return true;
    }
    
    public static void checkAttributeNotExists(Schema schema, String attributeName) {
        checkNotNull(schema, attributeName);
        if (schema.containsAttribute(attributeName)) {
            throw new TexeraException(ErrorMessages.DUPLICATE_ATTRIBUTE(
                    schema, attributeName));
        }
    }
    
    public static void checkAttributeNotExists(Schema schema, String... attributeNames) {
        checkNotNull(schema, attributeNames);
        checkAttributeNotExists(schema, Arrays.asList(attributeNames));
    }
    
    public static void checkAttributeNotExists(Schema schema, Iterable<String> attributeNames) {
        checkNotNull(schema, attributeNames);
        for (String attributeName : attributeNames) {
            checkAttributeNotExists(schema, attributeName);
        }
    }
    
    public static void checkAttributeExists(Schema schema, String attributeName) {
        checkNotNull(schema, attributeName);
        if (! schema.containsAttribute(attributeName)) {
            throw new TexeraException(ErrorMessages.DUPLICATE_ATTRIBUTE(
                    schema, attributeName));
        }
    }
    
    public static void checkAttributeExists(Schema schema, String... attributeNames) {
        checkNotNull(schema, attributeNames);
        checkAttributeExists(schema, Arrays.asList(attributeNames));
    }
    
    public static void checkAttributeExists(Schema schema, Iterable<String> attributeNames) {
        checkNotNull(schema, attributeNames);
        for (String attributeName : attributeNames) {
            checkAttributeExists(schema, attributeName);
        }
    }
    
    
    public static class Builder {
        
        private final ArrayList<Attribute> attributeList;
        private final HashSet<String> attributeNames;
        
        public Builder() {
            this.attributeList = new ArrayList<>();
            this.attributeNames = new HashSet<>();
        }
        
        public Builder(Schema schema) {
            checkNotNull(schema);
            this.attributeList = new ArrayList<>(schema.getAttributes());
            this.attributeNames = new HashSet<>(schema.getAttributes().stream()
                    .map(attr -> attr.getName().toLowerCase())
                    .collect(Collectors.toSet()));
        }
        
        private void checkAttributeNotExist(Attribute attribute) {
            if (attributeNames.contains(attribute.getName())) {
                throw new IllegalArgumentException(
                        "attribute " + attribute.getName() + " already exists in the schema");
            }
        }
        
        public Builder add(Attribute attribute) {
            checkNotNull(attribute);
            checkAttributeNotExist(attribute);
            this.attributeList.add(attribute);
            this.attributeNames.add(attribute.getName().toLowerCase());
            return this;
        }
        
        public Builder add(String attributeName, AttributeType attributeType) {
            checkNotNull(attributeName, attributeType);
            return this.add(new Attribute(attributeName, attributeType));
        }
        
        public Builder add(Attribute... attributes) {
            checkNotNull(attributes);
            for (int i = 0; i < attributes.length; i++) {
                this.add(attributes[i]);
            }
            return this;
        }
        
        public Builder addAll(Iterable<Attribute> attributes) {
            checkNotNull(attributes);
            for (Attribute attribute : attributes) {
                this.add(attribute);
            }
            return this;
        }
        
        public Builder addAll(Iterator<Attribute> attributes) {
            checkNotNull(attributes);
            while (attributes.hasNext()) {
                Attribute attribute = attributes.next();
                this.add(attribute);
            }   
            return this;
        }
        
        public Builder add(Schema schema) {
            checkNotNull(schema);
            this.addAll(schema.getAttributes());
            return this;
        }
        
        public Schema build() {
            return new Schema(this.attributeList);
        }
        
    }

}
