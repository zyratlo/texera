package edu.uci.ics.textdb.exp.twitterfeed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IDField;
import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;


import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by Chang on 7/7/17.
 */

/**
 * This TwitterFeedOperator sets up a connection with Twitter using the twitter streaming API
 * and stores the received tweet stream in a queue.
 * When getNextTuple() is called, pull a record from the queue and parse it into a tuple.
 *
 *------------------------------  (1).invoke  ------------------  (2).connect with BasicClient  ----------------
 * TwitterFeed Operator         |----------->| TwitterConnector |----------------------------->| TwitterService |
 *------------------------------ <----------- ------------------ <----------------------------- ----------------
 *                         (4).take tweets and process them      (3).receive tweet streams and
 *                                                                      store in a queue.
 *
 */
public class TwitterFeedOperator implements ISourceOperator {

    private final TwitterFeedSourcePredicate predicate;

    private final Schema outputSchema;
    private TwitterConnector twitterConnector;

    private int resultCursor;
    private int limit;
    private int timeout;
    private int cursor = CLOSED;

    private String msg;
    private Tuple sourceTuple;

    /** Secondary constructor for TwitterFeedOperator.
     * It will be called by the primary constructor upon operator set-up.
     * It allows customer-defined TwitterConnector in the input and
     * enables mockito testing to mock a twitterConnector and isolate the connection with twitter .
     */

    public TwitterFeedOperator(TwitterFeedSourcePredicate predicate, TwitterConnector twitterConnector) throws TextDBException {
        this.resultCursor = -1;
        this.limit = Integer.MAX_VALUE;
        this.timeout = 10;
        this.outputSchema = Utils.getSchemaWithID(TwitterUtils.twitterSchema.TWITTER_SCHEMA);
        this.predicate = predicate;
        this.twitterConnector = twitterConnector;
        if (timeout <= 0) {
            throw new DataFlowException("Please provide a positive timeout limit.");
        }
        if ((predicate.getQuery() == null || predicate.getQuery().isEmpty())
                && (predicate.getLocations() == null || predicate.getLocations().isEmpty())
                && (predicate.getLanguage() == null || predicate.getLanguage().isEmpty())) {
            throw new DataFlowException("no filter is provided");
        }

    }

    //Primary constructor for TwitterFeedOperator set up.
    public TwitterFeedOperator(TwitterFeedSourcePredicate predicate) throws TextDBException {
        this(predicate, new TwitterConnector(predicate.getQuery(), TwitterUtils.getPlaceLocation(predicate.getLocations()), predicate.getLanguage(), predicate.getCustomerKey(), predicate.getCustomerSecret(), predicate.getToken(), predicate.getTokenSecret()));
    }


    @Override
    public void open() throws TextDBException {
        if (cursor != CLOSED) {
            return;
        }

        try {

            twitterConnector.getClient().connect();
            cursor = OPENED;

        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
    }


    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (cursor == CLOSED || resultCursor >= limit - 1 || resultCursor >= predicate.getTweetNum() - 1) {
            return null;
        }

        if (twitterConnector.getClient().isDone()) {
            System.out.println("Client connection closed unexpectedly: " + twitterConnector.getClient().getExitEvent().getMessage());
            return null;
        }

        try {

            msg = twitterConnector.getMsgQueue().poll(timeout, TimeUnit.SECONDS);

            if (msg == null) {
                System.out.println("Did not receive a message in " + timeout + " seconds");
                return null;
            }
            JsonNode tweet = new ObjectMapper().readValue(msg, JsonNode.class);

            sourceTuple = new Tuple(outputSchema, IDField.newRandomID(),
                    new TextField(TwitterUtils.getText(tweet)),
                    new StringField(TwitterUtils.getTweetLink(tweet)),
                    new StringField(TwitterUtils.getUserLink(tweet)),
                    new TextField(TwitterUtils.getUserScreenName(tweet)),
                    new TextField(TwitterUtils.getUserName(tweet)),
                    new TextField(TwitterUtils.getUserDescription(tweet)),
                    new IntegerField(TwitterUtils.getUserFollowerCnt(tweet)),
                    new IntegerField(TwitterUtils.getUserFriendsCnt(tweet)),
                    new TextField(TwitterUtils.getUserLocation(tweet)),
                    new StringField(TwitterUtils.getCreateTime(tweet)),
                    new TextField(TwitterUtils.getPlaceName(tweet)),
                    new StringField(TwitterUtils.getCoordinates(tweet)),
                    new StringField(TwitterUtils.getLanguage(tweet)));
            resultCursor++;
            return sourceTuple;

        } catch (InterruptedException e) {
            System.out.println(e);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void close() throws TextDBException {
        if (cursor == CLOSED) {
            return;
        }

        twitterConnector.getClient().stop();
        cursor = CLOSED;
    }


    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
}


