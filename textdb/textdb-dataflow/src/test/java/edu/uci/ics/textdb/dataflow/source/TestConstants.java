/**
 * 
 */
package edu.uci.ics.textdb.dataflow.source;

import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.StringField;
import edu.uci.ics.textdb.dataflow.common.DataTuple;

/**
 * @author sandeepreddy602
 *
 */
public class TestConstants {
    // Sample Fields
    public static final String FIRST_NAME = "firstName";
    public static final String LAST_NAME = "lastName";

    // Sample Schema
    public static final List<String> SAMPLE_SCHEMA = Arrays.asList(FIRST_NAME,
            LAST_NAME);

    // Sample Tuples
    public static final List<ITuple> SAMPLE_TUPLES = Arrays.asList(
            new DataTuple(SAMPLE_SCHEMA, new StringField("f1"), new StringField("l1")), 
            new DataTuple(SAMPLE_SCHEMA, new StringField("f2"), new StringField("l2")),
            new DataTuple(SAMPLE_SCHEMA, new StringField("f3"), new StringField("l3")), 
            new DataTuple(SAMPLE_SCHEMA, new StringField("f4"), new StringField("l4")),
            new DataTuple(SAMPLE_SCHEMA, new StringField("f5"), new StringField("l5")));
}
