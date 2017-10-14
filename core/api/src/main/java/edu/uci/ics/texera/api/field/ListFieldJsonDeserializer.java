package edu.uci.ics.texera.api.field;

import java.io.IOException;
import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import edu.uci.ics.texera.api.constants.JsonConstants;
import edu.uci.ics.texera.api.span.Span;

// TODO: currently all List<T> are SpanList. In the future maybe 
//   either change List<T> to SpanList, or support generic list
public class ListFieldJsonDeserializer extends StdDeserializer<ListField<?>> {

    private static final long serialVersionUID = 6079052875639039779L;
    
    public ListFieldJsonDeserializer() {
        this(null);
    }

    protected ListFieldJsonDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public ListField<Span> deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        JsonNode node = p.getCodec().readTree(p);
        JsonNode fieldValueNode = node.get(JsonConstants.FIELD_VALUE);
        
        ArrayList<Span> spanList = new ArrayList<>();
        for (int i = 0; i < fieldValueNode.size(); i++) {
            JsonNode spanValueNode = fieldValueNode.get(i);
            spanList.add(new ObjectMapper().convertValue(spanValueNode, Span.class));
        }
        return new ListField<Span>(spanList);
    }

}
