package edu.uci.ics.textdb.common.field;

public class Span {
    //The name of the field (in the tuple) where this span is present
    private String fieldName;
    //The key we are searching for eg: regex
    private String key;
    //The value matching the key
    private String value;
    //The start of the span
    private int start;
    //The end of the span
    private int end;
    
    public Span(String fieldName, String key, String value, int start, int end) {
        this.fieldName = fieldName;
        this.key = key;
        this.value = value;
        this.start = start;
        this.end = end;
    }

    public String getFieldName() {
        return fieldName;
    }
    
    public String getKey() {
        return key;
    }
    
    public String getValue() {
        return value;
    }
    
    public int getStart() {
        return start;
    }
    
    public int getEnd() {
        return end;
    }
}
