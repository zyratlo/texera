package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.web.request.beans.RegexMatcherBean;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the RegexMatcher operators' properties
 * Created by kishorenarendran on 11/09/16.
 */
public class RegexMatcherBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        final RegexMatcherBean regexMatcherBean = new RegexMatcherBean("operator1", "RegexMatcher", "attributes",
                "10", "100", "regex");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator1\",\n" +
                "    \"operator_type\": \"RegexMatcher\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"regex\": \"regex\"\n" +
                "}";
        RegexMatcherBean deserializedObject = MAPPER.readValue(jsonString, RegexMatcherBean.class);
        assertEquals(regexMatcherBean.equals(deserializedObject), true);
    }

    @Test
    public void testInvalidDeserialization() throws IOException {
        final RegexMatcherBean regexMatcherBean = new RegexMatcherBean("operator1", "RegexMatcher", "attributes",
                "10", "100", "regex");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator2\",\n" +
                "    \"operator_type\": \"RegexMatcher\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"regex\": \"regex1\"\n" +
                "}";
        RegexMatcherBean deserializedObject = MAPPER.readValue(jsonString, RegexMatcherBean.class);
        assertEquals(regexMatcherBean.equals(deserializedObject), false);
    }
}