package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.common.constants.DataConstants;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of generic properties common to all operators
 * Created by kishorenarendran on 10/20/16.
 */
public class OperatorBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        final DictionaryMatcherBean dictionaryMatcherBean = new DictionaryMatcherBean("operator1", "DictionaryMatcher",
                "attributes", "10", "100", "dictionary", DataConstants.KeywordMatchingType.PHRASE_INDEXBASED);
        final OperatorBean operatorBean = dictionaryMatcherBean;
        String jsonString = "{\n" +
                "        \"operator_id\": \"operator1\",\n" +
                "        \"operator_type\": \"DictionaryMatcher\",\n" +
                "        \"attributes\": \"attributes\",\n" +
                "        \"limit\": \"10\",\n" +
                "        \"offset\": \"100\",\n" +
                "        \"dictionary\": \"dictionary\",\n" +
                "        \"matching_type\": \"PHRASE_INDEXBASED\"\n" +
                "    }";
        DictionaryMatcherBean deserializedObject = MAPPER.readValue(jsonString, DictionaryMatcherBean.class);
        OperatorBean deserializedOperatorBean = deserializedObject;
        assertEquals(operatorBean.equals(deserializedOperatorBean), true);
    }
}