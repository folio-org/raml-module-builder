package org.folio.rest.tools.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NaiveSQLParse {
/**
   * Return the last position of <code>token</code> in <code>query</code> skipping
   * standard SQL strings like 'some string' and C-style SQL strings like E'some string'.
   * @param query  where to search
   * @param token  what to search for
   * @return position (starting at 0), or -1 if not found
   */
  public static int getLastStartPos(String query, String token) {
    int found = -1;
    int i = 0;
    while (i < query.length() - 1) {
      boolean cEscape = false;
      if ((query.charAt(i) == 'E' || query.charAt(i) == 'e') && query.charAt(i+1) == '\'') {
        cEscape = true;
        i++;
      }
      char c = query.charAt(i);
      if (cEscape || c == '\'' || c == '\"') {
        char del = c;
        i++;
        while (i < query.length()) {
          c = query.charAt(i);
          i++;
          if (cEscape && c == '\\') {
            i++; // to at least skip \\ and \'
          } else if (c == del) {
            if (i < query.length() && query.charAt(i) == del) {
              i++;
            } else {
              break;
            }
          }
        }        
      } else {
        int i1 = i + token.length();
        if (i1 <= query.length()) {
          String sub = query.substring(i, i1);          
          boolean before = i == 0 || !Character.isJavaIdentifierPart(query.charAt(i - 1));
          boolean after = i1 >= query.length() || !Character.isJavaIdentifierPart(query.charAt(i1));
          if (before && after && sub.equalsIgnoreCase(token))  {
            found = i;
          }
        }
        i++;
      }
    }
    return found;
  }
}
