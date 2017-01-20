package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the TupleStreamSink operators' properties
 * Created by kishorenarendran on 1/19/17.
 */
public class TupleStreamSinkBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();
    private TupleStreamSinkBean tupleStreamSinkBean;
    private TupleStreamSinkBean deserializedObject;
    private OperatorBean operatorBean;
    private OperatorBean deserializedOperatorBean;

    @Test
    public void testDeserialization() throws IOException {
        tupleStreamSinkBean = new TupleStreamSinkBean("operator1", "TupleStreamSink",
                "attributes", "1000", "5");
        operatorBean = tupleStreamSinkBean;
        String jsonString = "{\n" +
                "        \"operator_id\": \"operator1\",\n" +
                "        \"operator_type\": \"TupleStreamSink\",\n" +
                "        \"attributes\": \"attributes\",\n" +
                "        \"limit\": \"1000\",\n" +
                "        \"offset\": \"5\"\n" +
                "    }";
        deserializedObject = MAPPER.readValue(jsonString, TupleStreamSinkBean.class);
        deserializedOperatorBean = deserializedObject;
        assertEquals(operatorBean.equals(deserializedOperatorBean), true);
    }

    @Test
    public void testInvalidDeserialization() throws IOException {
        tupleStreamSinkBean = new TupleStreamSinkBean("operator1", "DictionarySource",
                "attributes", "1000", "5");
        operatorBean = tupleStreamSinkBean;
        String jsonString = "{\n" +
                "        \"operator_id\": \"operator1\",\n" +
                "        \"operator_type\": \"TupleStreamSink\",\n" +
                "        \"attributes\": \"attributes1\",\n" +
                "        \"limit\": \"1000\",\n" +
                "        \"offset\": \"5\"\n" +
                "    }";
        deserializedObject = MAPPER.readValue(jsonString, TupleStreamSinkBean.class);
        deserializedOperatorBean = deserializedObject;
        assertEquals(operatorBean.equals(deserializedOperatorBean), false);
    }
}
