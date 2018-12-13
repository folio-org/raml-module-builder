package org.folio.rest.jaxrs.model;

public enum UsersGetOrder {
  DESC("desc"),

  ASC("asc");

  private String name;

  UsersGetOrder(String name) {
    this.name = name;
  }
}
