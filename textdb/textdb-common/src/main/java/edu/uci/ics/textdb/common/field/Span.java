package edu.uci.ics.textdb.common.field;

public class Span {
    //The name of the field (in the tuple) where this span is present
    private String fieldName;
    //The start of the span
    private int start;
    //The end of the span
    private int end;
    //The key we are searching for eg: regex
    private String key;
    //The value matching the key
    private String value;
    
    
    public Span(String fieldName, int start, int end, String key, String value) {
        this.fieldName = fieldName;
        this.start = start;
        this.end = end;
        this.key = key;
        this.value = value;
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + end;
        result = prime * result
                + ((fieldName == null) ? 0 : fieldName.hashCode());
        result = prime * result + start;
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
        Span other = (Span) obj;
        if (end != other.end)
            return false;
        if (fieldName == null) {
            if (other.fieldName != null)
                return false;
        } else if (!fieldName.equals(other.fieldName))
            return false;
        if (start != other.start)
            return false;
        return true;
    }
}
