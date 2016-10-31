package org.folio.rest.persist.mongo;


public enum DateEnum {

  MONTH("$month"), //number between 1 (January) and 12 (December).
  DAY_OF_YEAR("$dayOfYear"), //between 1 and 366 (leap year).
  DAY_OF_MONTH("$dayOfMonth"), //number between 1 and 31.
  DAY_OF_WEEK("$dayOfWeek"), //number between 1 and 7.
  YEAR("$year"), //ex 2070
  WEEK ("$week"),//between 0 (partial week precedes first Sunday of year) and 53 (leap year)
  HOUR ("$hour"), //ex 0-23
  MINUTE ("$minute"), //ex 0-59
  SECOND ("$second"), //ex 0-59
  MILLSECOND ("$millisecond"); //ex 0-999

  private String val;

  DateEnum(String date){
    val = date;
  }

  public String getValue(){
    return val;
  }
}
