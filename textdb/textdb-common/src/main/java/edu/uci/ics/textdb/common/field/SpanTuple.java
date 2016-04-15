package edu.uci.ics.textdb.common.field;

import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;

public class SpanTuple extends DataTuple{
    
    private IField spanField;
    private Span span;
    private String key;

    public SpanTuple(List<Attribute> schema, IField[] fields, 
            String key, IField spanField, Span span) {
        super(schema, fields);
        this.spanField = spanField;
        this.span = span;
        this.key = key;
    }

    public Span getSpan() {
        return span;
    }
    
    public IField getSpanField() {
        return spanField;
    }
    
    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "SpanTuple [spanField=" + spanField + ", span=" + span
                + ", key=" + key + "]";
    }    
}
