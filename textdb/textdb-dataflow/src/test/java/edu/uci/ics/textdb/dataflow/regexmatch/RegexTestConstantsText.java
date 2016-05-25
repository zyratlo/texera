package edu.uci.ics.textdb.dataflow.regexmatch;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.StringField;

public class RegexTestConstantsText {
    // Sample test data of some random text
    public static final String CONTENT = "content";
    
    public static final Attribute CONTENT_ATTR = new Attribute(CONTENT, FieldType.TEXT);

    public static final Attribute[] ATTRIBUTES_TEXT = { CONTENT_ATTR };
    public static final Schema SCHEMA_TEXT = new Schema(ATTRIBUTES_TEXT);
    
    private static ITuple getTextTuple(String content) {
    	IField field = new StringField(content);
    	ITuple tuple = new DataTuple(SCHEMA_TEXT, field);
    	return tuple;
    }
    
    public static List<ITuple> getSampleTextTuples() throws ParseException {
    	List<ITuple> textTuples = new ArrayList<>();
    	textTuples.add(getTextTuple("This testcase is for testing regex that can be translated by the translator"));
    	textTuples.add(getTextTuple("Translator is effective for specific regex, but has less effects on general regular expressions"));
    	textTuples.add(getTextTuple("Testing is really really important and we need to write a large amount of high quality tests"));
    	textTuples.add(getTextTuple("We need to make sure that corner cases are tested"));

    	textTuples.add(getTextTuple("The patient will be seen in followup. She will have a followup."));
    	textTuples.add(getTextTuple("The patient is to follow up with Dr. Smith. I will follow up the patient in two weeks."));
    	textTuples.add(getTextTuple("The patient will have a follow-up (or followup) examination."));
    	
    	return textTuples;
    }
}
