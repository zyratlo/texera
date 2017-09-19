package edu.uci.ics.texera.api.tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkNotNull;

import edu.uci.ics.texera.api.constants.JsonConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;

/**
 * @author chenli
 * @author sandeepreddy602
 * @author Zuozhi Wang
 * 
 * Created on 3/25/16.
 */
@JsonDeserialize(using = TupleJsonDeserializer.class)
public class Tuple {
    private final Schema schema;
    private final ImmutableList<IField> fields;

    public Tuple(Schema schema, IField... fields) {
        this(schema, Arrays.asList(fields));
    }
    
    @JsonCreator
    public Tuple(
            @JsonProperty(value = JsonConstants.SCHEMA, required = true)
            Schema schema, 
            @JsonProperty(value = JsonConstants.FIELDS, required = true)
            List<IField> fields) {
        // check arguments are not null
        checkNotNull(schema);
        checkNotNull(fields);
        fields.forEach(field -> checkNotNull(field));
        
        // check schema matches the fields
        checkSchemaMatchesFields(schema.getAttributes(), fields);

        this.schema = schema;
        this.fields = ImmutableList.copyOf(fields);
        if (this.fields == null) {
            throw new TexeraException("something goes wrong here");
        }
    }
    
    @JsonProperty(value = JsonConstants.SCHEMA)
    public Schema getSchema() {
        return schema;
    }
    
    @JsonProperty(value = JsonConstants.FIELDS)
    public List<IField> getFields() {
        return this.fields;
    }

    @SuppressWarnings("unchecked")
    public <T extends IField> T getField(String attributeName) {
        if (! schema.containsAttribute(attributeName)) {
            throw new TexeraException(attributeName + " is not in the tuple");
        }
        return (T) fields.get(schema.getIndex(attributeName));
    }
    
    public <T extends IField> T getField(String attributeName, Class<T> fieldClass) {
        return getField(attributeName);
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fields == null) ? 0 : fields.hashCode());
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Tuple other = (Tuple) obj;
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

    public String toString() {
        return "Tuple [schema=" + schema + ", fields=" + fields + "]";
    }
    
    public ObjectNode getReadableJson() {
        ObjectNode objectNode = new ObjectMapper().createObjectNode();
        for (String attrName : this.schema.getAttributeNames()) {
            objectNode.set(attrName, JsonNodeFactory.instance.pojoNode(this.getField(attrName).getValue()));
        }
        return objectNode;
    }
    
    private static void checkSchemaMatchesFields(List<Attribute> attributes, List<IField> fields) throws TexeraException {
        // check schema's size and field's size are the same
        if (attributes.size() != fields.size()) {
            throw new TexeraException(String.format(
                    "Schema size (%d) and field size (%d) are different", 
                    attributes.size(), fields.size()));
        }
        
        // check schema's type and field's type match
        for (int i = 0; i < fields.size(); i++) {
            checkAttributeMatchesField(attributes.get(i), fields.get(i));
        }
    }
    
    private static void checkAttributeMatchesField(Attribute attribute, IField field) throws TexeraException {
        if (! field.getClass().equals(attribute.getType().getFieldClass())) {
            throw new TexeraException(String.format(
                    "Attribute %s's type (%s) is different from field's type (%s)", 
                    attribute.getName(), attribute.getType(),
                    AttributeType.getAttributeType(field.getClass())));
        }
    }
    
    public static class Builder {
        
        private final Schema.Builder schemaBuilder;
        private final HashMap<String, IField> fieldNameMap;
        
        public Builder() {
            this.schemaBuilder = new Schema.Builder();
            this.fieldNameMap = new HashMap<>();
        }
        
        public Builder(Tuple tuple) {
            checkNotNull(tuple);
            checkNotNull(tuple.getFields());
            
            this.schemaBuilder = new Schema.Builder(tuple.getSchema());
            this.fieldNameMap = new HashMap<>();
            for (int i = 0; i < tuple.getFields().size(); i++) {
                this.fieldNameMap.put(
                        tuple.getSchema().getAttributes().get(i).getName().toLowerCase(), 
                        tuple.fields.get(i));
            }
        }
        
        public Tuple build() {
            Schema schema = schemaBuilder.build();
            ArrayList<IField> fields = new ArrayList<>();
            for (int i = 0; i < schema.getAttributes().size(); i++) {
                fields.add(fieldNameMap.get(schema.getAttributes().get(i).getName().toLowerCase()));
            }
            return new Tuple(schema, fields);
        }
        
        public Builder add(Attribute attribute, IField field) {
            checkNotNull(attribute);
            checkNotNull(field);
            checkAttributeMatchesField(attribute, field);
            
            // schema builder will check if the attribute already exists
            schemaBuilder.add(attribute);
            fieldNameMap.put(attribute.getName().toLowerCase(), field);
            return this;
        }
        
        public Builder add(String attributeName, AttributeType attributeType, IField field) {
            checkNotNull(attributeName);
            checkNotNull(attributeType);
            checkNotNull(field);
            
            this.add(new Attribute(attributeName, attributeType), field);
            return this;
        }
        
        public Builder add(List<Attribute> attributes, List<IField> fields) {
            checkNotNull(attributes);
            attributes.forEach(attr -> checkNotNull(attr));
            checkNotNull(fields);
            fields.forEach(field -> checkNotNull(field));
            
            checkSchemaMatchesFields(attributes, fields);
            for (int i = 0; i < attributes.size(); i++) {
                this.add(attributes.get(i), fields.get(i));
            }
            return this;
        }
        
        public Builder remove(String attribute) {
            checkNotNull(attribute);
            
            // schemaBuilder will check if the attribute exists
            schemaBuilder.remove(attribute);
            fieldNameMap.remove(attribute.toLowerCase());  
            return this;
        }
        
        public Builder remove(List<String> attributes) {
            checkNotNull(attributes);
            attributes.forEach(attr -> checkNotNull(attr));
            
            attributes.forEach(attr -> this.remove(attr));
            return this;
        }
        
        public Builder remove(String... attributes) {
            checkNotNull(attributes);
            
            remove(Arrays.asList(attributes));
            return this;
        }
        
        public Builder removeIfExists(String attribute) {
            checkNotNull(attribute);
            
            schemaBuilder.removeIfExists(attribute);
            fieldNameMap.remove(attribute.toLowerCase());
            return this;
        }
        
        public Builder removeIfExists(List<String> attributes) {
            checkNotNull(attributes);
            attributes.forEach(attr -> checkNotNull(attr));
            
            attributes.forEach(attr -> this.removeIfExists(attr));
            return this;
        }
        
        public Builder removeIfExists(String... attributes) {
            checkNotNull(attributes);
            
            removeIfExists(Arrays.asList(attributes));
            return this;
        }
        
        /*********************
         * public static helper functions to handle a list of tuples
         *********************/
        
        public static List<Tuple> remove(List<Tuple> tuples, String attribute) {
            checkNotNull(tuples);
            tuples.forEach(tuple -> checkNotNull(tuple));
            checkNotNull(attribute);
            
            return tuples.stream()
                    .map(tuple -> new Tuple.Builder(tuple))
                    .map(builder -> builder.remove(attribute))
                    .map(builder -> builder.build())
                    .collect(Collectors.toList());
        }
        
        public static List<Tuple> remove(List<Tuple> tuples, List<String> attributes) {
            checkNotNull(tuples);
            tuples.forEach(tuple -> checkNotNull(tuple));
            checkNotNull(attributes);
            attributes.forEach(attr -> checkNotNull(attr));
            
            return tuples.stream()
                    .map(tuple -> new Tuple.Builder(tuple))
                    .map(builder -> builder.remove(attributes))
                    .map(builder -> builder.build())
                    .collect(Collectors.toList());
        }
        
        public static List<Tuple> remove(List<Tuple> tuples, String... attributes) {
            return(remove(tuples, Arrays.asList(attributes)));
        }
        
        public static List<Tuple> removeIfExists(List<Tuple> tuples, String attribute) {
            checkNotNull(tuples);
            tuples.forEach(tuple -> checkNotNull(tuple));
            checkNotNull(attribute);
            
            return tuples.stream()
                    .map(tuple -> new Tuple.Builder(tuple))
                    .map(builder -> builder.removeIfExists(attribute))
                    .map(builder -> builder.build())
                    .collect(Collectors.toList());
        }

        public static List<Tuple> removeIfExists(List<Tuple> tuples, List<String> attributes) {
            checkNotNull(tuples);
            tuples.forEach(tuple -> checkNotNull(tuple));
            checkNotNull(attributes);
            attributes.forEach(attr -> checkNotNull(attr));
            
            return tuples.stream()
                    .map(tuple -> new Tuple.Builder(tuple))
                    .map(builder -> builder.removeIfExists(attributes))
                    .map(builder -> builder.build())
                    .collect(Collectors.toList());
        }
        
        public static List<Tuple> removeIfExists(List<Tuple> tuples, String... attributes) {
            return(removeIfExists(tuples, Arrays.asList(attributes)));
        }
        
    }
    
}
