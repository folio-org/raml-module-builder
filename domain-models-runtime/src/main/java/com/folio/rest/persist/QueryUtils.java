package com.folio.rest.persist;

import io.vertx.core.json.JsonObject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Splitter;

public class QueryUtils {

  private static final Pattern  QUERY_PARSER  = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
  private static final Splitter SPLITTER      = Splitter.on(QUERY_PARSER);
  private static final Pattern  WITHIN_QUOTES = Pattern.compile("\"([^\"]*)\"");

  public static JsonObject createQueryFilter(JsonObject query) {
    JsonObject ret = new JsonObject();
    if (query == null) {
      return ret;
    }
    if ("".equals(query.getString("query"))) {
      return ret;
    }
    try {
      Set<Map.Entry<String, Object>> clauseSet = query.getMap().entrySet();
      Iterator<Map.Entry<String, Object>> iter = clauseSet.iterator();
      while (iter.hasNext()) {
        Map.Entry<String, Object> entry = iter.next();
        if (entry.getValue() == null) {
          continue;
        }
        String clause = entry.getValue().toString();
        List<String> queryClause = SPLITTER.splitToList(clause);
        /*
         * Matcher m = QUERY_PARSER.matcher(clause); String queryClause[] = new
         * String[3]; int index = 0; while (m.find()) { queryClause[index] =
         * m.group(m.groupCount()); index++; }
         */

        if (queryClause.size() == 1) {
          // this is a key:value filter so just add it to the query
          // object
          // no need to check further
          ret.put(entry.getKey(), entry.getValue());
        } else {
          String operation = null;
          String field = null;
          String value = null;
          Matcher m = WITHIN_QUOTES.matcher(queryClause.get(2));
          while (m.find()) {
            operation = m.group(1);
          }
          m = WITHIN_QUOTES.matcher(queryClause.get(0));
          while (m.find()) {
            field = m.group(1);
          }
          m = WITHIN_QUOTES.matcher(queryClause.get(1));
          while (m.find()) {
            value = m.group(1);
          }
          switch (operation) {
            case "=":
              ret.put(field, value);
              break;
            case "<":
              ret.put(field, new JsonObject("{ \"$lt\": " + queryClause.get(1) + " }"));
              break;
            case ">":
              ret.put(field, new JsonObject("{ \"$gt\": " + queryClause.get(1) + " }"));
              break;
            case "<=":
              ret.put(field, new JsonObject("{ \"$lte\": " + queryClause.get(1) + " }"));
              break;
            case "!=":
              ret.put(field, new JsonObject("{ \"$ne\": " + queryClause.get(1) + " }"));
              break;
            case "=>":
              ret.put(field, new JsonObject("{ \"$gte\": " + queryClause.get(1) + " }"));
              break;
          }

        }

      }

    } catch (Exception e) {
      System.out.println("unsupported query, send back a warning and skip");
    }
    return ret;

  }

}
