
package org.folio.rest.jaxrs.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Null;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


/**
 * Collection of users
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "users",
    "totalRecords",
    "resultInfo"
})
public class UserdataCollection {

    /**
     * List of userdata items
     * (Required)
     *
     */
    @JsonProperty("users")
    @JsonPropertyDescription("List of userdata items")
    @Valid
    @NotNull
    private List<User> users = new ArrayList<User>();
    /**
     *
     * (Required)
     *
     */
    @JsonProperty("totalRecords")
    @NotNull
    private Integer totalRecords;
    @JsonProperty("resultInfo")
    @Null
    @Valid
    private ResultInfo resultInfo;
    @JsonIgnore
    @Valid
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    /**
     * List of userdata items
     * (Required)
     *
     */
    @JsonProperty("users")
    public List<User> getUsers() {
        return users;
    }

    /**
     * List of userdata items
     * (Required)
     *
     */
    @JsonProperty("users")
    public void setUsers(List<User> users) {
        this.users = users;
    }

    public UserdataCollection withUsers(List<User> users) {
        this.users = users;
        return this;
    }

    /**
     *
     * (Required)
     *
     */
    @JsonProperty("totalRecords")
    public Integer getTotalRecords() {
        return totalRecords;
    }

    /**
     *
     * (Required)
     *
     */
    @JsonProperty("totalRecords")
    public void setTotalRecords(Integer totalRecords) {
        this.totalRecords = totalRecords;
    }

    public UserdataCollection withTotalRecords(Integer totalRecords) {
        this.totalRecords = totalRecords;
        return this;
    }

    @JsonProperty("resultInfo")
    public ResultInfo getResultInfo() {
        return resultInfo;
    }

    @JsonProperty("resultInfo")
    public void setResultInfo(ResultInfo resultInfo) {
        this.resultInfo = resultInfo;
    }

    public UserdataCollection withResultInfo(ResultInfo resultInfo) {
        this.resultInfo = resultInfo;
        return this;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    public UserdataCollection withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

}
