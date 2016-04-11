/**
 * 
 */
package edu.uci.ics.textdb.common.constants;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.field.Attribute;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.DateField;
import edu.uci.ics.textdb.common.field.DoubleField;
import edu.uci.ics.textdb.common.field.FieldType;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.StringField;

/**
 * @author sandeepreddy602
 * Including this class in src/main/java since it is required by other projects
 * Keeping it in src/test/java doesn't make it available to the other projects
 */
public class TestConstants {
    // Sample Fields
    public static final String FIRST_NAME = "firstName";
    public static final String LAST_NAME = "lastName";
    public static final String AGE = "age";
    public static final String HEIGHT = "height";
    public static final String DATE_OF_BIRTH = "dateOfBirth";
    
    
    public static final Attribute FIRST_NAME_ATTR = new Attribute(FIRST_NAME, FieldType.STRING);
    public static final Attribute LAST_NAME_ATTR = new Attribute(LAST_NAME, FieldType.STRING);
    public static final Attribute AGE_ATTR = new Attribute(AGE, FieldType.INTEGER);
    public static final Attribute HEIGHT_ATTR = new Attribute(HEIGHT, FieldType.DOUBLE);
    public static final Attribute DATE_OF_BIRTH_ATTR = new Attribute(DATE_OF_BIRTH, FieldType.DATE);

    // Sample Schema
    public static final List<Attribute> SAMPLE_SCHEMA = Arrays.asList(
            FIRST_NAME_ATTR, LAST_NAME_ATTR, AGE_ATTR, HEIGHT_ATTR, DATE_OF_BIRTH_ATTR);
    
    public static List<ITuple> getSampleTuples() throws ParseException{
        
        IField[] fields1 = {new StringField("sandeep"), new StringField("madugula"), new IntegerField(27), 
                new DoubleField(5.70), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1989"))};
        IField[] fields2 = {new StringField("rajesh"), new StringField("yarlagadda"), new IntegerField(24), 
                new DoubleField(5.95), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1992"))};
        IField[] fields3 = {new StringField("sudeep"), new StringField("meduri"), new IntegerField(25), 
                new DoubleField(5.71), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1991"))};
        IField[] fields4 = {new StringField("chen"), new StringField("li"), new IntegerField(41), 
                new DoubleField(5.65), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1975"))};
        IField[] fields5 = {new StringField("jianfeng"), new StringField("jia"), new IntegerField(26), 
                new DoubleField(5.69), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1990"))};
        
        
        
        return Arrays.asList(new DataTuple(SAMPLE_SCHEMA, fields1), 
                new DataTuple(SAMPLE_SCHEMA, fields2), new DataTuple(SAMPLE_SCHEMA, fields3), 
                new DataTuple(SAMPLE_SCHEMA, fields4), new DataTuple(SAMPLE_SCHEMA, fields5) );
                
    }
}
