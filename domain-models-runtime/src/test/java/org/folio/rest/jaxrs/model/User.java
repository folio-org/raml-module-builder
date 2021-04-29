
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
    "_version",
    "metadata",
    "dummy"
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
     * Record version for optimistic locking
     *
     */
    @JsonProperty("_version")
    @JsonPropertyDescription("Record version for optimistic locking")
    private Integer version;

    /**
     * Metadata Schema
     * <p>
     * Metadata about creation and changes to records, provided by the server (client should not provide)
     *
     */
    @JsonProperty("metadata")
    @JsonPropertyDescription("Metadata about creation and changes to records, provided by the server (client should not provide)")
    @javax.validation.constraints.Null
    private Metadata metadata;

    /**
     * A dummy field to be set by testing trigger
     *
     */
    @JsonProperty("dummy")
    @JsonPropertyDescription("A dummy field to be set by testing trigger")
    private String dummy;


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

    /**
     * Record version for optimistic locking
     *
     */
    @JsonProperty("_version")
    public Integer getVersion() {
        return version;
    }

    /**
     * Record version for optimistic locking
     *
     */
    @JsonProperty("_version")
    public void setVersion(Integer version) {
        this.version = version;
    }

    public User withVersion(Integer version) {
        this.version = version;
        return this;
    }

    /**
     * Metadata Schema
     * <p>
     * Metadata about creation and changes to records, provided by the server (client should not provide)
     *
     */
    @JsonProperty("metadata")
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * Metadata Schema
     * <p>
     * Metadata about creation and changes to records, provided by the server (client should not provide)
     *
     */
    @JsonProperty("metadata")
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public String getDummy() {
      return dummy;
    }

    public void setDummy(String dummy) {
      this.dummy = dummy;
    }

}
