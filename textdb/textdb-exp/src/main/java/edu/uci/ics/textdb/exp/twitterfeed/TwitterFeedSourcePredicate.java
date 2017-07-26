package edu.uci.ics.textdb.exp.twitterfeed;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

import java.util.List;

/**
 * Created by Chang on 7/12/17.
 */

/**
 * Twitter streaming API supports "trackTerms" on keyword, location and language.
 * These query parameters along with the number of tweets required
 * can be specified in TwitterFeedSourcePredicate.
 */
public class TwitterFeedSourcePredicate extends PredicateBase {

    private final int tweetNum;
    private final List<String> keywordList;
    private final String locationList;
    private final List<String> languageList;


    public TwitterFeedSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.TWEET_NUM, required = true)
                    int tweetNum,
            @JsonProperty(value = PropertyNameConstants.TWEET_QUERY_LIST, required = false)
                    List<String> keywordList,
            @JsonProperty(value = PropertyNameConstants.TWEET_LOCATION_LIST, required = false)
                    String locationList,
            @JsonProperty(value = PropertyNameConstants.TWEET_LANGUAGE_LIST, required = false)
                    List<String> languageList) {

        this.tweetNum = tweetNum;
        this.keywordList = keywordList;
        this.locationList = locationList;
        this.languageList = languageList;


    }

    @JsonProperty(PropertyNameConstants.TWEET_QUERY_LIST)
    public List<String> getQuery() {
        return this.keywordList;
    }

    @JsonProperty(PropertyNameConstants.TWEET_LOCATION_LIST)
    public String getLocations() {
        return this.locationList;
    }

    @JsonProperty(PropertyNameConstants.TWEET_NUM)
    public int getTweetNum() {
        return this.tweetNum;
    }

    @JsonProperty(PropertyNameConstants.TWEET_LANGUAGE_LIST)
    public List<String> getLanguage() {
        return this.languageList;
    }


    @Override
    public IOperator newOperator() {
        return new TwitterFeedOperator(this);
    }


}
