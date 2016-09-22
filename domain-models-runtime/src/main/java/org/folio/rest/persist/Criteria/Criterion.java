package org.folio.rest.persist.Criteria;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author shale
 *
 */
public class Criterion {

  private static final ObjectMapper mapper           = new ObjectMapper();

  private String                    snippet          = "";

  private Order                     order            = new Order();

  private Limit                     limit            = new Limit();

  private Offset                    offset           = new Offset();

  private boolean                   whereClauseAdded = false;

  HashMap<String, Select>           selects          = new HashMap<>();

  HashMap<String, From>             froms            = new HashMap<>();

  public Criterion() {

  }

  private void updateSnippet() {
    if (snippet.length() > 0) {
      snippet = snippet + " AND "; // default to AND between criterion - make
                                   // this controllable in the future
    }
  }

  public Criterion(Criteria a) {
    updateSnippet();
    snippet = snippet + a;
    addCriteriaInfo(a);
  }

  public Criterion addCriterion(Criteria a, String op, Criteria b) {
    updateSnippet();
    snippet = snippet + "(" + a + " " + op + " " + b + ")";
    addCriteriaInfo(a);
    addCriteriaInfo(b);
    return this;
  }

  public Criterion addCriterion(Criteria a) {
    updateSnippet();
    snippet = snippet + a;
    addCriteriaInfo(a);
    return this;
  }

  private void addCriteriaInfo(Criteria a) {
    if (a != null) {
      if (a.from != null) {
        froms.put(a.from.snippet, a.from);
      }
      if (a.select != null) {
        selects.put(a.select.snippet, a.select);
      }
    }
  }

  /*
   * public Criterion addCriterion(Criterion a, String op, Criteria b){
   * updateSnippet(); snippet = snippet + "(" + addPrefix( a.toString() ) + " "
   * + op + " " + addPrefix( b.toString() ) + ")"; return this; }
   *
   * public Criterion addCriterion(Criterion a, String op, Criterion b){
   * updateSnippet(); snippet = snippet + "(" + addPrefix(a.toString()) + " " +
   * op + " " + addPrefix( b.toString() ) + ")"; return this; }
   */

  public Criterion setOrder(Order order) {
    this.order = order;
    return this;
  }

  public Criterion setLimit(Limit limit) {
    this.limit = limit;
    return this;
  }

  public Criterion setOffset(Offset offset) {
    this.offset = offset;
    return this;
  }

  @Override
  public String toString() {
    if (!whereClauseAdded && snippet.length() > 0) {
      snippet = " WHERE " + snippet;
      whereClauseAdded = true;
    }
    return snippet + " " + order.toString() + " " + offset.toString() + " " + limit.toString();
  }

  public static Criterion json2Criterion(String query) {
    Criterion cc = new Criterion();
    try {
      if(query != null){
        JsonNode node = mapper.readTree(query);
        Iterator<JsonNode> iter = node.elements();
        processQueryIntern(iter, cc);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return cc;
  }

  private static Criterion processQueryIntern(Iterator<JsonNode> iter, Criterion cc) {
    try {
      int clauseCount = 0;
      Criteria c[] = new Criteria[2];
      String op = null;
      int pos = 0;
      while (iter.hasNext()) {
        JsonNode jsonNode = iter.next();
        if (jsonNode.isArray()) {
          processQueryIntern(jsonNode.elements(), cc);
        } else {
          clauseCount++;
          if (1 == jsonNode.size()) {
            op = jsonNode.get("op").textValue();
          } else {
            Criteria crit = new Criteria();
            String[] fields = jsonNode.get("field").textValue().split("->>|->");
            int field2remove = -1;
            for (int i = 0; i < fields.length; i++) {
              if ("[]".equals(fields[i])) {
                crit.isArray = true;
                field2remove = i;
              }
            }
            ArrayList<String> fieldList = new ArrayList<String>(Arrays.asList(fields));
            if (field2remove != -1) {
              fieldList.remove(field2remove);
            }
            crit.field = fieldList;
            crit.value = jsonNode.get("value");
            crit.operation = jsonNode.get("op").textValue();
            c[pos++] = crit;
          }
        }
      }
      if (clauseCount == 3) {
        cc.addCriterion(c[0], op, c[1]);
      } else if (clauseCount == 2) {
        // not query
        c[0].isNotQuery = true;
        cc.addCriterion(c[0]);
      } else if (clauseCount == 1) {
        cc.addCriterion(c[0]);
      }
      return cc;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return cc;
  }

  public String selects2String() {
    StringBuilder sb = new StringBuilder();

    Set<Map.Entry<String, Select>> entries = selects.entrySet();
    Iterator<Entry<String, Select>> entry = entries.iterator();
    int count = 0;
    int size = selects.size();
    while (entry.hasNext()) {
      Map.Entry<String, Select> entry2 = (Map.Entry<String, Select>) entry.next();
      Select select = entry2.getValue();
      sb.append(select.snippet).append(" ");
      if (select.asValue != null) {
        sb.append(" AS ").append(select.asValue).append(" ");
      }
      if (++count < size) {
        sb.append(", ");
      }
    }
    return sb.toString();
  }

  public String from2String() {
    StringBuilder sb = new StringBuilder();

    Set<Map.Entry<String, From>> entries = froms.entrySet();
    Iterator<Entry<String, From>> entry = entries.iterator();
    int count = 0;
    int size = froms.size();
    while (entry.hasNext()) {
      Map.Entry<String, From> entry2 = (Map.Entry<String, From>) entry.next();
      From from = entry2.getValue();
      sb.append(from.snippet).append(" ");
      ;
      if (from.asValue != null) {
        sb.append(" AS ").append(from.asValue).append(" ");
      }
      if (++count < size) {
        sb.append(", ");
      }
    }
    return sb.toString();
  }

  public static void main(String args[]) {

    Criterion a = json2Criterion("[{\"field\":\"'fund_distributions'->[]->'amount'->>'sum'\",\"value\":120,\"op\":\"<\"}]");
    // System.out.println(a.toString());
    Criteria b = new Criteria();
    b.field.add("'note'");
    b.operation = "=";
    b.value = "a";
    b.isArray = true;

    Criteria c = new Criteria();
    c.addField("'price'").addField("'po_currency'").addField("'value'");
    c.operation = "like";
    c.value = "USD";
    //c.isArray = true;

    Criteria d = new Criteria();
    d.field.add("'rush'");
    d.operation = Criteria.OP_IS_FALSE;
    d.value = null;
    /*
     * Criteria a = new Criteria(); a.field = "'rush'"; a.operation = "!=";
     * a.value = "true";
     *
     * Criteria aa = new Criteria(); aa.field = "'rush'"; aa.operation = "=";
     * aa.value = null;
     */

    // Criterion cc = new Criterion();
    // cc.addCriterion(aa);
    // cc.addCriterion(c, "OR", b);
    // cc.addCriterion(a);
    a.addCriterion(c, "OR", b);
    a.addCriterion(d);
    /*
     * Criterion bb = new Criterion(); bb.addCriterion(cc, "AND", a);
     */

    System.out.println("SELECT " + a.selects2String() + " FROM " + a.from2String() + a.toString());
  }

}
