package edu.uci.ics.texera.api.field;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

import edu.uci.ics.texera.api.constants.JsonConstants;

public class DateField implements IField {

    private String localDateTimeString;

    public DateField(Date value) {
        checkNotNull(value);
        this.localDateTimeString = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm").format(value);
    }

    public DateField(LocalDateTime localDateTime) {
        checkNotNull(localDateTime);
        this.localDateTimeString = localDateTime.toString();
    }

    @JsonCreator
    public DateField(
            @JsonProperty(value = JsonConstants.FIELD_VALUE, required = true) 
            String localDateTimeString) {
        checkNotNull(localDateTimeString);
        this.localDateTimeString = localDateTimeString;
    }

    @JsonProperty(value = JsonConstants.FIELD_VALUE)
    public String getDateString() {
        return this.localDateTimeString;
    }

    @JsonIgnore
    @Override
    public LocalDateTime getValue() {
        return LocalDateTime.parse(this.localDateTimeString);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((localDateTimeString == null) ? 0 : localDateTimeString.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DateField other = (DateField) obj;
        if (localDateTimeString == null) {
            if (other.localDateTimeString != null)
                return false;
        } else if (!localDateTimeString.equals(other.localDateTimeString))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "DateField [value=" + localDateTimeString + "]";
    }

}
