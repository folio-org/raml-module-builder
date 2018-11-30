package org.folio.rest.jaxrs.model;

public enum CalendarPeriodsServicePointIdCalculateopeningGetUnit {
  DAY("day"),

  HOUR("hour"),

  BEEINTERVAL("bee interval");

  private String name;

  CalendarPeriodsServicePointIdCalculateopeningGetUnit(String name) {
    this.name = name;
  }
}
