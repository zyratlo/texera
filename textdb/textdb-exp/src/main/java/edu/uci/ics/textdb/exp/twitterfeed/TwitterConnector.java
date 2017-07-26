package edu.uci.ics.textdb.exp.twitterfeed;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by Chang on 7/12/17.
 */

/**
 * This class is to build up connection with Twitter using twitter streaming API.
 * Details of this API: https://dev.twitter.com/streaming/overview
 * Twitter streaming API supports "trackTerms" on keyword, location and language.
 * These three query domains can not be empty at the same time.
 */
public class TwitterConnector {

    private BasicClient twitterClient;
    private BlockingQueue<String> messageQueue;

    public TwitterConnector(List<String> queryList, List<Location> locationList, List<String> languageList) throws TextDBException {
        if ((queryList == null || queryList.isEmpty()) && (locationList == null || locationList.isEmpty())
                && (languageList == null || languageList.isEmpty())) {
            throw new DataFlowException("no filter is provided");
        }
        messageQueue = new LinkedBlockingQueue<String>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        if (!(queryList == null || queryList.isEmpty())) {
            endpoint.trackTerms(queryList);
        }
        if (!(locationList == null || locationList.isEmpty())) {
            endpoint.locations(locationList);
        }
        if (!(languageList == null || languageList.isEmpty())) {
            endpoint.languages(languageList);
        }

        Authentication auth = new OAuth1(TwitterUtils.twitterConstant.consumerKey, TwitterUtils.twitterConstant.consumerSecret, TwitterUtils.twitterConstant.token, TwitterUtils.twitterConstant.tokenSecret);

        twitterClient = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(messageQueue))
                .build();
    }

    public BasicClient getClient() {
        return twitterClient;
    }

    public void setMessageQueue(BlockingQueue queue) {
        this.messageQueue = queue;
    }

    public BlockingQueue<String> getMsgQueue() {
        return messageQueue;
    }

}
