package org.folio.rest.persist.helpers;
/**
 * @author shale
 *
 */
public enum SQLFunctions {

  AVG("AVG"),
  COUNT("COUNT"),
  FIRST("FIRST"),
  LAST("LAST"),
  MAX("MAX"),
  MIN("MIN"),
  SUM("SUM"),
  GROUP_BY("GROUP BY"),
  HAVING("HAVING"),
  UCASE("UCASE"),
  LCASE("LCASE"),
  MID("MID"),
  LEN("LEN"),
  ROUND("ROUND"),
  NOW("NOW"),
  FORMAT("FORMAT");

  private String func;

  SQLFunctions(String str){
    func = str;
  }

  public static boolean contains(String test) {

    for (SQLFunctions c : SQLFunctions.values()) {
        if (test.toUpperCase().startsWith(c.name())) {
            return true;
        }
    }

    return false;
}

}
