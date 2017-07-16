package edu.uci.ics.textdb.api.field;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.constants.JsonConstants;

/**
 * Created by chenli on 3/31/16.
 */
public interface IField {
    
    @JsonProperty(value = JsonConstants.FIELD_VALUE)
    Object getValue();
}
