package edu.uci.ics.textdb.perftest.twitter;

import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;

/**
 * Created by Vinay on 08-04-2017.
 */
public class TwitterSchema {


    public static final String ID = "tweet_id";
    public static final String CONTENT = "tweet_text";

    public static final Attribute ID_ATTR = new Attribute(ID, AttributeType.STRING);
    public static final Attribute CONTENT_ATTR = new Attribute(CONTENT, AttributeType.TEXT);

    public static final Schema TWEET_SCHEMA = new Schema(ID_ATTR, CONTENT_ATTR);


}
