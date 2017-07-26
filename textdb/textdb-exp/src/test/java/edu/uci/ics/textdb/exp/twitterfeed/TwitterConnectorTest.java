package edu.uci.ics.textdb.exp.twitterfeed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Created by Chang on 7/25/17.
 */

/**
 * This test aims at the successful connection from the
 * BasicClient defined in twitter streaming API to twitter.
 * Variable "exist" sets a bound to check if twitterConnector
 * can successfully receive messages from twitter through the
 * connection built up with the BasicClient inside it to twitter
 * and if messages received are not empty.
 */
public class TwitterConnectorTest {
    public TwitterConnector twitterConnector;

    @Before
    public void setUp() throws Exception {
        String dummyKeyword = "is";
        List<String> keyWordList = new ArrayList<>(Arrays.asList(dummyKeyword));
        twitterConnector = new TwitterConnector(keyWordList, null, null);
        twitterConnector.getClient().connect();

    }

    @After
    public void tearDown() throws Exception {
        twitterConnector.getClient().stop();
    }

    @Test
    public void testTwitterConnector() throws Exception {
        int tweetCount = 0;
        int exist = 2;
        while (tweetCount < exist) {
            String message = twitterConnector.getMsgQueue().take();
            assertTrue(!TwitterUtils.getUserScreenName(new ObjectMapper().readValue(message, JsonNode.class)).isEmpty());
            tweetCount++;
        }
    }
}
