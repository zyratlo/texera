package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the Links between operators
 * Created by kishorenarendran on 10/20/16.
 */
public class OperatorLinkBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        final OperatorLinkBean operatorLinkBean = new OperatorLinkBean("operator1", "operator2");
        String jsonString = "{\n" +
                "    \"from\": \"operator1\",\n" +
                "    \"to\": \"operator2\"\n" +
                "}";
        OperatorLinkBean deserializedObject = MAPPER.readValue(jsonString, OperatorLinkBean.class);
        assertEquals(operatorLinkBean.getToOperatorID().equals(deserializedObject.getToOperatorID()), true);
        assertEquals(operatorLinkBean.getFromOperatorID().equals(deserializedObject.getFromOperatorID()), true);
    }
}