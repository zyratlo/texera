package edu.uci.ics.textdb.dataflow.neextractor;

import edu.uci.ics.textdb.api.common.*;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.field.*;
import junit.framework.Assert;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Sam on 16/4/27.
 */
public class NEExtractorTestConstants {


    public static final String SENTENCE="sentence";

    public static final Attribute SENTENCE_ATTR = new Attribute(SENTENCE, FieldType.TEXT);


    public static final List<Attribute> ATTRIBUTES_ONE_SENTENCE = Arrays.asList(
            SENTENCE_ATTR);

    public static final List<Attribute> ATTRIBUTES_TWO_SENTENCE = Arrays.asList(
            SENTENCE_ATTR);
    public static final Schema SCHEMA_ONE_SENTENCE = new Schema(ATTRIBUTES_ONE_SENTENCE);
    public static final Schema SCHEMA_TWO_SENTENCE = new Schema(ATTRIBUTES_TWO_SENTENCE);




    public static List<ITuple> getTest1Tuples() throws ParseException {
        IField[] fields1 = {new TextField("Microsoft is a organization.")};
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }



    public static List<ITuple> getTest2Tuples() throws ParseException {


        IField [] fields1 = {new TextField("Microsoft, Google and Facebook are organizations.")};
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }

    public static List<ITuple> getTest3Tuples() throws ParseException {

        IField [] fields1 = {new TextField("Microsoft,Google and Facebook are organizations and Donald Trump and Barack Obama are persons.")};
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }

    public static List<ITuple> getTest4Tuples() throws ParseException {

        IField [] fields1 = {new TextField("Microsoft,Google and Facebook are organizations."), new TextField(" Donald Trump and Barack Obama are persons")};
        ITuple tuple1 = new DataTuple(SCHEMA_TWO_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }


    public static ITuple getTest1ResultTuple() {
        List<Attribute> attributes = new ArrayList<Attribute>();
        attributes.add(SchemaConstants.SPAN_LIST_ATTRIBUTE);
        List<IField> fields = new ArrayList<IField>();
        List<Span> list = new ArrayList<Span>();
        Span span1 = new Span("description", 0, 8, "ORGANIZATION", "Microsoft");
        list.add(span1);
        IField spanField = new ListField<Span>(list);
        fields.add(spanField);
        ITuple resultTuple = new DataTuple(new Schema(attributes), fields.toArray(new IField[fields.size()]));
        return resultTuple;
    }

    public static ITuple getTest2ResultTuple() {
        List<Attribute> attributes = new ArrayList<Attribute>();
        attributes.add(SchemaConstants.SPAN_LIST_ATTRIBUTE);
        List<IField> fields = new ArrayList<IField>();
        List<Span> list = new ArrayList<Span>();
        Span span1 = new Span("description", 0, 8, "ORGANIZATION", "Microsoft");
        Span span2 = new Span("description", 10, 15, "ORGANIZATION", "Google");
        Span span3 = new Span("description", 21, 28, "ORGANIZATION", "Facebook");

        list.add(span1);
        list.add(span2);
        list.add(span3);
        IField spanField = new ListField<Span>(list);
        fields.add(spanField);
        ITuple resultTuple = new DataTuple(new Schema(attributes), fields.toArray(new IField[fields.size()]));
        return resultTuple;
    }

    public static ITuple getTest3ResultTuple() {
        List<Attribute> attributes = new ArrayList<Attribute>();
        attributes.add(SchemaConstants.SPAN_LIST_ATTRIBUTE);
        List<IField> fields = new ArrayList<IField>();
        List<Span> list = new ArrayList<Span>();
        Span span1 = new Span("description", 0, 8, "ORGANIZATION", "Microsoft");
        Span span2 = new Span("description", 10, 15, "ORGANIZATION", "Google");
        Span span3 = new Span("description", 21, 28, "ORGANIZATION", "Facebook");
        Span span4 = new Span("description", 52, 63, "PERSON", "Donald Trump");
        Span span5 = new Span("description", 69, 80, "PERSON", "Barack Obama");

        list.add(span1);
        list.add(span2);
        list.add(span3);
        list.add(span4);
        list.add(span5);

        IField spanField = new ListField<Span>(list);
        fields.add(spanField);
        ITuple resultTuple = new DataTuple(new Schema(attributes), fields.toArray(new IField[fields.size()]));
        return resultTuple;
    }



}
