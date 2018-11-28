
package org.folio.rest.jaxrs.model;

import javax.validation.constraints.NotNull;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


/**
 * User Schema
 * <p>
 * A user
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "username",
    "id",
})
public class User {

    /**
     * A unique name belonging to a user. Typically used for login
     *
     */
    @JsonProperty("username")
    @JsonPropertyDescription("A unique name belonging to a user. Typically used for login")
    private String username;
    /**
     * A globally unique (UUID) identifier for the user
     * (Required)
     *
     */
    @JsonProperty("id")
    @JsonPropertyDescription("A globally unique (UUID) identifier for the user")
    @NotNull
    private String id;

    /**
     * A unique name belonging to a user. Typically used for login
     *
     */
    @JsonProperty("username")
    public String getUsername() {
        return username;
    }

    /**
     * A unique name belonging to a user. Typically used for login
     *
     */
    @JsonProperty("username")
    public void setUsername(String username) {
        this.username = username;
    }

    public User withUsername(String username) {
        this.username = username;
        return this;
    }

    /**
     * A globally unique (UUID) identifier for the user
     * (Required)
     *
     */
    @JsonProperty("id")
    public String getId() {
        return id;
    }

    /**
     * A globally unique (UUID) identifier for the user
     * (Required)
     *
     */
    @JsonProperty("id")
    public void setId(String id) {
        this.id = id;
    }

    public User withId(String id) {
        this.id = id;
        return this;
    }
}
