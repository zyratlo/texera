package edu.uci.ics.textdb.exp.twitter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.source.asterix.AsterixSource;

public class TwitterConverter implements IOperator {
    
    private final String rawDataAttribute = AsterixSource.RAW_DATA;
    
    private IOperator inputOperator;
    private Schema outputSchema;
    private int cursor = CLOSED;
    
    public TwitterConverter() {
    }
    
    public void setInputOperator(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }

    @Override
    public void open() throws TextDBException {
        if (cursor == OPENED) {
            return;
        }
        if (inputOperator == null) {
            throw new DataFlowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputOperator.open();
        outputSchema = transformSchema(inputOperator.getOutputSchema());
        cursor = OPENED;
    }

    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (cursor == CLOSED) {
            throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        Tuple tuple;
        while ((tuple = inputOperator.getNextTuple()) != null) {
            List<IField> tweetFields = generateFieldsFromJson(
                    tuple.getField(rawDataAttribute).getValue().toString());
            if (! tweetFields.isEmpty()) {
                cursor++;
                
                List<IField> tupleFields = new ArrayList<>();
                
                final Tuple finalTuple = tuple;
                tupleFields.addAll(tuple.getSchema().getAttributeNames().stream()
                        .filter(attrName -> ! attrName.equalsIgnoreCase(rawDataAttribute))
                        .map(attrName -> finalTuple.getField(attrName, IField.class))
                        .collect(Collectors.toList()));
                tupleFields.addAll(tweetFields);
                return new Tuple(outputSchema, tupleFields);
            }
        }
        return null;
    }
    
    private List<IField> generateFieldsFromJson(String rawJsonData) {
        try {
            JsonNode tweet = new ObjectMapper().readTree(rawJsonData);
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
            return Arrays.asList(
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
        } catch (Exception e) {
            System.out.println("cannot convert tweet, skipping");
            return Arrays.asList();
        }
    }

    @Override
    public void close() throws TextDBException {
        if (cursor == CLOSED) {
            return;
        }
        inputOperator.close();
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }
    
    private Schema transformSchema(Schema inputSchema) {
        if (! inputSchema.containsField(rawDataAttribute)) {
            throw new DataFlowException(String.format(
                    "raw twitter attribute %s is not present in the input schema %s",
                    rawDataAttribute, inputSchema.getAttributeNames()));
        }
        ArrayList<Attribute> outputAttributes = new ArrayList<>();
        outputAttributes.addAll(inputSchema.getAttributes().stream()
                .filter(attr -> ! attr.getAttributeName().equalsIgnoreCase(rawDataAttribute))
                .collect(Collectors.toList()));
        outputAttributes.addAll(TwitterConverterConstants.additionalAttributes);
        return new Schema(outputAttributes);
    }

}
