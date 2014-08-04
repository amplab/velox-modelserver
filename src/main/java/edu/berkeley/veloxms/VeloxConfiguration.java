package edu.berkeley.veloxms;

import io.dropwizard.Configuration;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

public class VeloxConfiguration extends Configuration {
    @NotEmpty
    private String itemModelLoc;

    @NotEmpty
    private String userModelLoc;

    @NotEmpty
    private String ratingsLoc;


    @JsonProperty
    public String getItemModelLoc() {
        return itemModelLoc;
    }

    @JsonProperty
    public void setItemModelLoc(String itemModelLoc) {
        this.itemModelLoc = itemModelLoc;
    }

    @JsonProperty
    public String getUserModelLoc() {
        return userModelLoc;
    }

    @JsonProperty
    public void setUserModelLoc(String userModelLoc) {
        this.userModelLoc = userModelLoc;
    }

    @JsonProperty
    public String getRatingsLoc() {
        return ratingsLoc;
    }

    @JsonProperty
    public void setRatingsLoc(String ratingsLoc) {
        this.ratingsLoc = ratingsLoc;
    }

}
