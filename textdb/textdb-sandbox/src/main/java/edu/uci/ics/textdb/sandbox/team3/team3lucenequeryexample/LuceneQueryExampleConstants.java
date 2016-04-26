package edu.uci.ics.textdb.sandbox.team3.team3lucenequeryexample;

import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.Schema;

public class LuceneQueryExampleConstants {
	public static final String DATA = "data";
    public static final String ID = "id";
    
    public static final Attribute DATA_ATTR = new Attribute(DATA, FieldType.STRING);
    public static final Attribute ID_ATTR = new Attribute(ID, FieldType.INTEGER);
    
    public static final List<Attribute> ATTRIBUTES_DOC = Arrays.asList(
    		ID_ATTR, DATA_ATTR);
    public static final Schema SCHEMA_DOC = new Schema(ATTRIBUTES_DOC);
}
