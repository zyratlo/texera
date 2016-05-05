package edu.uci.ics.textdb.common.utils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexableField;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.DateField;
import edu.uci.ics.textdb.common.field.DoubleField;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;

public class Utils {
    public static IField getField(FieldType fieldType, String fieldValue) throws ParseException{
        IField field = null;
        switch (fieldType) {
            case STRING:
                field = new StringField(fieldValue);
                break;
            case INTEGER:
                field = new IntegerField(Integer.parseInt(fieldValue));
                break;
            case DOUBLE:
                field = new DoubleField(Double.parseDouble(fieldValue));
                break;
            case DATE:
                field = new DateField(DateTools.stringToDate(fieldValue));
                break;
            case TEXT:
                field = new TextField(fieldValue);
                break;
            
            default:
                break;
        }
        return field;
    }

    public static IndexableField getLuceneField(FieldType fieldType,
            String fieldName, Object fieldValue) {
        IndexableField luceneField = null;
        switch(fieldType){
	        case STRING:
                luceneField = new org.apache.lucene.document.StringField(
                        fieldName, (String) fieldValue, Store.YES);
                break;
            case INTEGER:
                luceneField = new org.apache.lucene.document.IntField(
                        fieldName, (Integer) fieldValue, Store.YES);
                break;
            case DOUBLE:
                double value = (Double) fieldValue;
                luceneField = new org.apache.lucene.document.DoubleField(
                        fieldName, value, Store.YES);
                break;
            case DATE:
                String dateString = DateTools.dateToString((Date) fieldValue, Resolution.MILLISECOND);
                luceneField = new org.apache.lucene.document.StringField(fieldName, dateString, Store.YES);
                break;
            case TEXT:
	            luceneField = new org.apache.lucene.document.TextField(
	                    fieldName, (String) fieldValue, Store.YES);
	            break;
            
        }
        return luceneField;
    }
    /**
     * @about Modifies schema, fields and creates a new span tuple
     */
    public static ITuple getSpanTuple( List<IField> fieldList, List<Span> spanList, Schema spanSchema) {
        IField spanListField = new ListField<Span>(new ArrayList<>(spanList));
        List<IField> fieldListDuplicate = new ArrayList<>(fieldList);
        fieldListDuplicate.add(spanListField);

        IField[] fieldsDuplicate = fieldListDuplicate.toArray(new IField[fieldListDuplicate.size()]);
        return new DataTuple(spanSchema, fieldsDuplicate);
    }
    
    /**
     * 
     * @param schema 
     * @about Creating a new schema object, and adding SPAN_LIST_ATTRIBUTE to
     *        the schema. SPAN_LIST_ATTRIBUTE is of type List
     */
    public static Schema createSpanSchema(Schema schema) {
        List<Attribute> dataTupleAttributes = schema.getAttributes();
        //spanAttributes contains all attributes of dataTupleAttributes and an additional SPAN_LIST_ATTRIBUTE
        Attribute[] spanAttributes = new Attribute[dataTupleAttributes.size() + 1];
        for (int count = 0; count < dataTupleAttributes.size(); count++) {
            spanAttributes[count] = dataTupleAttributes.get(count);
        }
        spanAttributes[spanAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;
        Schema spanSchema = new Schema(spanAttributes);
        return spanSchema;
    }
}
