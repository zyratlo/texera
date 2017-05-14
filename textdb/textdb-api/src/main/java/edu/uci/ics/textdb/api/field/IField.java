package edu.uci.ics.textdb.api.field;

import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.constants.JsonConstants;
import edu.uci.ics.textdb.api.schema.AttributeType;

/**
 * Created by chenli on 3/31/16.
 */
public interface IField {
    
    @JsonProperty(value = JsonConstants.FIELD_VALUE)
    Object getValue();
}
