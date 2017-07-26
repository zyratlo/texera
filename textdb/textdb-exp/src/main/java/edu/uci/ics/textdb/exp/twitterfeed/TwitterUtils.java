package edu.uci.ics.textdb.exp.twitterfeed;


import com.twitter.hbc.core.endpoint.Location;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import com.fasterxml.jackson.databind.JsonNode;
import com.twitter.hbc.core.endpoint.Location.Coordinate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Chang on 7/11/17.
 */


/**
 * Define Json parser functions, twitter schema, twitter constants such as consumer key, token and secret.
 */
public class TwitterUtils {

    public static String getCoordinates(JsonNode object) {
        if (object.hasNonNull("coordinates")) {
            JsonNode coord = object.get("coordinates").get("coordinates");
            return String.valueOf(coord.get(0).asDouble()) + ", " + String.valueOf(coord.get(1).asDouble());
        }
        return "n/a";
    }

    public static String getUserName(JsonNode object) {
        return object.get("user").get("name").asText();
    }

    public static String getUserScreenName(JsonNode object) {
        return object.get("user").get("screen_name").asText();
    }

    public static String getUserDescription(JsonNode object) {

        JsonNode user = object.get("user");
        if (user.hasNonNull("description")) {
            return user.get("description").asText();
        }
        return "n/a";
    }

    public static int getUserFollowerCnt(JsonNode object) {
        return object.get("user").get("followers_count").asInt();
    }

    public static int getUserFriendsCnt(JsonNode object) {
        return object.get("user").get("friends_count").asInt();
    }

    public static String getUserLocation(JsonNode object) {
        JsonNode user = object.get("user");
        if (user.hasNonNull("location")) {
            return user.get("location").asText();
        }
        return "n/a";

    }

    public static String getUserLink(JsonNode object) {
        return "https://twitter.com/" + getUserScreenName(object);
    }

    public static String getPlaceName(JsonNode object) {
        if (object.hasNonNull("place")) {
            JsonNode place = object.get("place");
            String placeName = place.get("full_name").asText();
            String country = place.get("country").asText();
            return placeName + ", " + country;
        }
        return "n/a";
    }

    public static String getCreateTime(JsonNode object) {
        return object.get("created_at").asText();
    }

    public static String getLanguage(JsonNode object) {
        return object.get("lang").asText();
    }

    public static String getTexts(JsonNode object) {
        return object.get("text").asText();
    }

    public static String getTweetLink(JsonNode object) {
        String tweetLink = "https://twitter.com/statuses/" + object.get("id_str").asText();
        return tweetLink;

    }

    /**
     * To track tweets inside a geoBox defined by a List</Location>.
     * The string defines the coordinates in the order of "latitude_SW, longitude_SW, latitude_NE, longitude_NE".
     * @param inputLocation
     * @return
     * @throws TextDBException
     */
    public static List<Location> getPlaceLocation(String inputLocation) throws TextDBException {
        List<Location> locationList = new ArrayList<>();
        if (inputLocation == null || inputLocation.isEmpty()) {
            return locationList;
        }
        List<String> boudingCoordinate = Arrays.asList(inputLocation.trim().split(","));
        if (boudingCoordinate.size() != 4 || boudingCoordinate.stream().anyMatch(s -> s.trim().isEmpty())) {
            throw new DataFlowException("Please provide valid location coordinates");
        }
        List<Double> boundingBox = new ArrayList<>();
        boudingCoordinate.stream().forEach(s -> boundingBox.add(Double.parseDouble(s.trim())));

        locationList.add(new Location(new Coordinate(boundingBox.get(1), boundingBox.get(0)), new Coordinate(boundingBox.get(3), boundingBox.get(2))));
        return locationList;
    }


    public static class twitterSchema {

        public static String TEXT = "text";
        public static Attribute TEXT_ATTRIBUTE = new Attribute(TEXT, AttributeType.TEXT);

        public static String TWEET_LINK = "tweet_link";
        public static Attribute TWEET_LINK_ATTRIBUTE = new Attribute(TWEET_LINK, AttributeType.STRING);

        public static String USER_LINK = "user_link";
        public static Attribute USER_LINK_ATTRIBUTE = new Attribute(USER_LINK, AttributeType.STRING);

        public static String USER_SCREEN_NAME = "user_screen_name";
        public static Attribute USER_SCREEN_NAME_ATTRIBUTE = new Attribute(USER_SCREEN_NAME, AttributeType.TEXT);

        public static String USER_NAME = "user_name";
        public static Attribute USER_NAME_ATTRIBUTE = new Attribute(USER_NAME, AttributeType.TEXT);

        public static String USER_DESCRIPTION = "user_description";
        public static Attribute USER_DESCRIPTION_ATTRIBUTE = new Attribute(USER_DESCRIPTION, AttributeType.TEXT);

        public static String USER_FOLLOWERS_COUNT = "user_followers_count";
        public static Attribute USER_FOLLOWERS_COUNT_ATTRIBUTE = new Attribute(USER_FOLLOWERS_COUNT, AttributeType.INTEGER);

        public static String USER_FRIENDS_COUNT = "user_friends_count";
        public static Attribute USER_FRIENDS_COUNT_ATTRIBUTE = new Attribute(USER_FRIENDS_COUNT, AttributeType.INTEGER);


        public static String PROFILE_LOCATION = "profile_location";
        public static Attribute PROFILE_LOCATION_ATTRIBUTE = new Attribute(PROFILE_LOCATION, AttributeType.TEXT);

        public static String CREATE_AT = "create_at";
        public static Attribute CREATE_AT_ATTRIBUTE = new Attribute(CREATE_AT, AttributeType.STRING);

        public static String TWEET_LOCATION = "tweet_place_name";
        public static Attribute TWEET_LOCATION_ATTRIBUTE = new Attribute(TWEET_LOCATION, AttributeType.TEXT);

        public static String TWEET_COORDINATES = "tweet_coordinates";
        public static Attribute TWEET_COORDINATES_ATTRIBUTE = new Attribute(TWEET_COORDINATES, AttributeType.STRING);

        public static String LANGUAGE = "lang";
        public static Attribute LANGUAGE_ATTRIBUTE = new Attribute(LANGUAGE, AttributeType.STRING);

        public static Schema TWITTER_SCHEMA = new Schema(
                TEXT_ATTRIBUTE, TWEET_LINK_ATTRIBUTE, USER_LINK_ATTRIBUTE,
                USER_SCREEN_NAME_ATTRIBUTE, USER_NAME_ATTRIBUTE, USER_DESCRIPTION_ATTRIBUTE,
                USER_FOLLOWERS_COUNT_ATTRIBUTE, USER_FRIENDS_COUNT_ATTRIBUTE,
                PROFILE_LOCATION_ATTRIBUTE, CREATE_AT_ATTRIBUTE, TWEET_LOCATION_ATTRIBUTE, TWEET_COORDINATES_ATTRIBUTE, LANGUAGE_ATTRIBUTE);


    }

    /**
     * Defines consumerKey and token for twitterClient connection.
     */

    public static class twitterConstant {
        public static final String consumerKey = "iJI9uxE1EKFlDwWOJnB1nvl2J";
        public static final String consumerSecret = "DjcNacjs9KOwO3w9zfBSWNJF96yerBj3GrgsJaMdERaWrG0a28";
        public static final String token = "884194955031306240-MucmXV5HBt9gFZfJ5WqGJZa11fhNTKT";
        public static final String tokenSecret = "WCEREP8SV6lhTacL1wvlluFkLRZGqBIiCC9fNLjbpM0Lt";
    }

}




