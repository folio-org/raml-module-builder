package org.folio.rest.jaxrs.model;

/**
 * Record of the table "referencing". referencing.userId is a foreign key to
 * users.id.
 */
public class Referencing {
  public String id;
  public String userId;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Referencing withId(String id) {
    this.id = id;
    return this;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public Referencing withUserId(String userId) {
    this.userId = userId;
    return this;
  }
}