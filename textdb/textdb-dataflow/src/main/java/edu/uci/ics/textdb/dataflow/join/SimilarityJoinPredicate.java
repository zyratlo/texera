package edu.uci.ics.textdb.dataflow.join;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IDField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.IJoinPredicate;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;

public class SimilarityJoinPredicate implements IJoinPredicate {
    
    public static final String INNER_PREFIX = "inner_";
    public static final String OUTER_PREFIX = "outer_";
    
    Double similarity;
    String joinAttributeName;
    
    /**
     * The output schema of Similarity Join will be the combination of inner and outer schema.
     *   (except the _id, spanList and payload field)
     *   
     * Example of a same-table, different-tuple join
     * table_schema,   inner_tuple,             outer_tuple
     *   _id:          random_id                random_id
     *   content:      "join"                   "john"
     *   spanList:     (0, 4, "join", content)  (0, 4, "john", content)
     *   payload:      payload_inner            payload_outer
     *   
     * result_schema,      result_tuple
     *   _id:              new_random_id
     *   inner_content:    "join"
     *   outer_content:    "john"
     *   spanList:         [(0, 4, "join", inner_content)  (0, 4, "john", outer_content)]
     *   payload:          [payload_inner, payload_outer]
     *   
     */
    public SimilarityJoinPredicate(String joinAttributeName, Double similarity) {
        if (similarity > 1) {
            similarity = 1.0;
        } else if (similarity < 0) {
            similarity = 0.0;
        }
        this.similarity = similarity;
        this.joinAttributeName = joinAttributeName;
    }

    @Override
    public String getIDAttributeName() {
        return SchemaConstants._ID;
    }

    @Override
    public String getJoinAttributeName() {
        return joinAttributeName;
    }
    
    @Override
    public Schema generateOutputSchema(Schema innerOperatorSchema, Schema outerOperatorSchema) throws DataFlowException {
        List<Attribute> outputAttributeList = new ArrayList<>();
        
        // add _ID field first
        outputAttributeList.add(SchemaConstants._ID_ATTRIBUTE);
        
        for (Attribute attr : innerOperatorSchema.getAttributes()) {
            String attrName = attr.getFieldName();
            FieldType attrType = attr.getFieldType();
            // ignore _id, spanList, and payload
            if (attrName.equals(SchemaConstants._ID) || attrName.equals(SchemaConstants.SPAN_LIST) 
                    || attrName.equals(SchemaConstants.PAYLOAD)) {
                continue;
            }
            outputAttributeList.add(new Attribute(INNER_PREFIX + attrName, attrType));
        }
        for (Attribute attr : outerOperatorSchema.getAttributes()) {
            String attrName = attr.getFieldName();
            FieldType attrType = attr.getFieldType();
            // ignore _id, spanList, and payload
            if (attrName.equals(SchemaConstants._ID) || attrName.equals(SchemaConstants.SPAN_LIST) 
                    || attrName.equals(SchemaConstants.PAYLOAD)) {
                continue;
            }
            outputAttributeList.add(new Attribute(OUTER_PREFIX + attrName, attrType));
        }
        
        // add spanList field
        outputAttributeList.add(SchemaConstants.SPAN_LIST_ATTRIBUTE);

        // add payload field if one of them contains payload
        if (innerOperatorSchema.containsField(SchemaConstants.PAYLOAD) || 
                outerOperatorSchema.containsField(SchemaConstants.PAYLOAD)) {
            outputAttributeList.add(SchemaConstants.PAYLOAD_ATTRIBUTE);
        }
        
        return new Schema(outputAttributeList.stream().toArray(Attribute[]::new));
    }

    @Override
    public ITuple joinTuples(ITuple outerTuple, ITuple innerTuple, Schema outputSchema) throws DataFlowException {        
        if (similarity == 0) {
            return null;
        }
        
        // get the span list only with the joinAttributeName
        List<Span> innerRelevantSpanList = ((ListField<Span>) innerTuple.getField(SchemaConstants.SPAN_LIST))
                .getValue().stream().filter(span -> span.getFieldName().equals(joinAttributeName)).collect(Collectors.toList());
        List<Span> outerRelevantSpanList = ((ListField<Span>) outerTuple.getField(SchemaConstants.SPAN_LIST))
                .getValue().stream().filter(span -> span.getFieldName().equals(joinAttributeName)).collect(Collectors.toList());
        
        // get a set of span's values (since multiple spans may have the same value)
        Set<String> innerSpanValueSet = innerRelevantSpanList.stream()
                .map(span -> span.getValue()).collect(Collectors.toSet());
        Set<String> outerSpanValueSet = outerRelevantSpanList.stream()
                .map(span -> span.getValue()).collect(Collectors.toSet());
        
        // compute the result value set using the distance function
        NormalizedLevenshtein distanceFunc = new NormalizedLevenshtein();
        Set<String> resultValueSet = new HashSet<>();
        for (String innerString : innerSpanValueSet) {
            for (String outerString : outerSpanValueSet) {
                Double distance = distanceFunc.distance(innerString, outerString);
                if (1 - distance >= similarity) {
                    resultValueSet.add(innerString);
                    resultValueSet.add(outerString);
                }
            }
        }
        
        // return null if none of them are similar
        if (resultValueSet.isEmpty()) {
            return null;
        }
        
        // generate the result spans
        List<Span> resultSpans = new ArrayList<>();
        for (Span span : innerRelevantSpanList) {
            if (resultValueSet.contains(span.getValue())) {
                resultSpans.add(addFieldPrefix(span, INNER_PREFIX));
            }
        }
        for (Span span : outerRelevantSpanList) {
            if (resultValueSet.contains(span.getValue())) {
                resultSpans.add(addFieldPrefix(span, OUTER_PREFIX));
            }
        }
                
        return mergeTuples(outerTuple, innerTuple, outputSchema, resultSpans);
    }
    
    
    private ITuple mergeTuples(ITuple outerTuple, ITuple innerTuple, Schema outputSchema, List<Span> mergeSpanList) {
        List<IField> resultFields = new ArrayList<>();
        for (String attrName : outputSchema.getAttributeNames()) {
            // generate a new _ID field for this tuple
            if (attrName.equals(SchemaConstants._ID)) {
                IDField innerID = (IDField) innerTuple.getField(SchemaConstants._ID);
                IDField outerID = (IDField) innerTuple.getField(SchemaConstants._ID);
                IDField newID = new IDField(innerID.getValue() + outerID.getValue());
                resultFields.add(newID);
            // use the generated spanList
            } else if (attrName.equals(SchemaConstants.SPAN_LIST)) {
                resultFields.add(new ListField<Span>(mergeSpanList));
            // put the payload of two tuples together
            } else if (attrName.equals(SchemaConstants.PAYLOAD)) {
                List<Span> innerPayload = ((ListField<Span>) innerTuple.getField(SchemaConstants.PAYLOAD)).getValue();
                List<Span> outerPayload = ((ListField<Span>) outerTuple.getField(SchemaConstants.PAYLOAD)).getValue();
                
                List<Span> resultPayload = new ArrayList<>();
                resultPayload.addAll(innerPayload.stream().map(span -> addFieldPrefix(span, INNER_PREFIX)).collect(Collectors.toList()));
                resultPayload.addAll(outerPayload.stream().map(span -> addFieldPrefix(span, "outer_")).collect(Collectors.toList()));
            // add other fields from inner/outer tuples
            } else {
                if (attrName.startsWith(INNER_PREFIX)) {
                    resultFields.add(innerTuple.getField(attrName.substring(INNER_PREFIX.length())));
                } else if (attrName.startsWith(OUTER_PREFIX)) {
                    resultFields.add(outerTuple.getField(attrName.substring(OUTER_PREFIX.length())));
                }
            }
        }
        return new DataTuple(outputSchema, resultFields.stream().toArray(IField[]::new));
    }
    
    private Span addFieldPrefix(Span span, String prefix) {
        return new Span(prefix+span.getFieldName(), 
                span.getStart(), span.getEnd(), span.getKey(), span.getValue(), span.getTokenOffset());
    }

}
