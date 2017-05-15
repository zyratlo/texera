package edu.uci.ics.textdb.perftest.twitter;

import java.io.File;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

public class TwitterSample {
    
    public static String twitterFilePath = PerfTestUtils.getResourcePath("/sample-data-files/twitter/tweets.json");
    public static String twitterClimateTable = "twitter";
    
    public static void main(String[] args) throws Exception {
        writeTwitterIndex();
    }
    
    public static void writeTwitterIndex() throws Exception {
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTable(twitterClimateTable);
        relationManager.createTable(twitterClimateTable, "../index/twitter/", TwitterSchema.TWITTER_SCHEMA, 
                LuceneAnalyzerConstants.standardAnalyzerString());
        
        DataWriter dataWriter = relationManager.getTableDataWriter(twitterClimateTable);
        dataWriter.open();
        
        int counter = 0;
        JsonNode jsonNode = new ObjectMapper().readTree(new File(twitterFilePath));
        for (JsonNode tweet : jsonNode) {
            try {
                String text = tweet.get("text").asText();
                Long id = tweet.get("id").asLong();
                String tweetLink = "https://twitter.com/statuses/" + id;
                JsonNode userNode = tweet.get("user");
                String userScreenName = userNode.get("screen_name").asText();
                String userLink = "https://twitter.com/" + userScreenName;
                String userName = userNode.get("name").asText();
                String userDescription = userNode.get("description").asText();
                Integer userFollowersCount = userNode.get("followers_count").asInt();
                Integer userFriendsCount = userNode.get("friends_count").asInt();
                JsonNode geoTagNode = tweet.get("geo_tag");
                String state = geoTagNode.get("stateName").asText();
                String county = geoTagNode.get("countyName").asText();
                String city = geoTagNode.get("cityName").asText();
                String createAt = tweet.get("create_at").asText();
                Tuple tuple = new Tuple(TwitterSchema.TWITTER_SCHEMA,
                        new TextField(text),
                        new StringField(tweetLink),
                        new StringField(userLink),
                        new TextField(userScreenName),
                        new TextField(userName),
                        new TextField(userDescription),
                        new IntegerField(userFollowersCount),
                        new IntegerField(userFriendsCount),
                        new TextField(state),
                        new TextField(county),
                        new TextField(city),
                        new StringField(createAt));
                dataWriter.insertTuple(tuple);
                counter++;
            } catch (RuntimeException e) {
                e.printStackTrace();
                continue;
            }
        }
        
        dataWriter.close();
        System.out.println("write twitter data finished");
        System.out.println(counter + " tweets written");
    }

}
