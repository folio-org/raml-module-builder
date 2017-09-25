package org.folio.rest.persist.Criteria;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.folio.rest.persist.Criteria.GroupedCriterias.Pairs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class Criterion {

  private static final ObjectMapper MAPPER           = new ObjectMapper();

  HashMap<String, Select>           selects          = new HashMap<>();

  HashMap<String, From>             froms            = new HashMap<>();

  private String                    snippet          = "";
  private Order                     order            = new Order();
  private Limit                     limit            = new Limit();
  private Offset                    offset           = new Offset();
  private boolean                   whereClauseAdded = false;
  private boolean                   isJoinCriterion  = false;

  public Criterion() {

  }

  public Criterion(Criteria a) {
    updateSnippet();
    snippet = snippet + a;
    addCriteriaInfo(a);
  }

  private void updateSnippet(String op) {
    if (snippet.length() > 0) {
      snippet = snippet + " " +op+ " "; // default to AND between criterion - make
                                   // this controllable in the future
    }
  }

  private void updateSnippet() {
    updateSnippet("AND");
  }

  /**
   *
   * @param a
   * @param op - operation between the two criterias
   * @param b
   * @param queryOp - operation of the created criterion, for example (a OR b) in relation to the entire query
   * for example: (a OR b) OR (b AND c)
   * @return
   */
  public Criterion addCriterion(Criteria a, String op, Criteria b, String queryOp) {
    updateSnippet(queryOp);
    snippet = snippet + "(" + a + " " + op + " " + b + ")";
    addCriteriaInfo(a);
    addCriteriaInfo(b);
    return this;
  }

  public Criterion addCriterion(Criteria a, String op, Criteria b) {
    updateSnippet();
    snippet = snippet + "(" + a + " " + op + " " + b + ")";
    addCriteriaInfo(a);
    addCriteriaInfo(b);
    return this;
  }

  public Criterion addCriterion(Criteria a, String queryOp) {
    updateSnippet(queryOp);
    snippet = snippet + a;
    addCriteriaInfo(a);
    return this;
  }

  public Criterion addCriterion(Criteria a) {
    updateSnippet();
    snippet = snippet + a;
    addCriteriaInfo(a);
    return this;
  }

  public Criterion addGroupOfCriterias(GroupedCriterias groupOfCriterias) {
    int size = groupOfCriterias.criterias.size();
    for (int i = 0; i < size; i++) {
      Pairs p = groupOfCriterias.criterias.get(i);
      if(i==0){
        String adjustedOp = groupOfCriterias.groupOp;
        if("NOT".equals(adjustedOp)){
          adjustedOp = "AND NOT";
        }
        if(snippet.length() == 0){
          adjustedOp = "";
        }
        snippet = snippet + " " + adjustedOp + " (";
        p.op = ""; //first op is ignored ( AND ...) doesn't make sense
      }
      addCriterion(p.criteria, p.op);
      if(i==size-1){
        snippet = snippet + ") ";
      }
    }
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
      if(a.isJoinON()){
        this.isJoinCriterion = true;
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
      String where = " WHERE ";
      if(isJoinCriterion){
        where = " ON ";
      }
      snippet = where + snippet;
      whereClauseAdded = true;
    }
    return snippet + " " + order.toString() + " " + offset.toString() + " " + limit.toString();
  }

  /**
   * example of json that can be passed in and converted into a postgres jsonb query
   * "[{\"field\":\"'fund_distributions'->[]->'amount'->>'sum'\",\"value\":120,\"op\":\"<\"}]"
   * with the use of cql this function should be depreicated
   * @param query
   * @return
   * @throws IOException
   * @throws JsonProcessingException
   */
  public static Criterion json2Criterion(String query) throws Exception {
    Criterion cc = new Criterion();
    if(query != null){
      JsonNode node = MAPPER.readTree(query);
      Iterator<JsonNode> iter = node.elements();
      processQueryIntern(iter, cc);
    }
    return cc;
  }

  private static Criterion processQueryIntern(Iterator<JsonNode> iter, Criterion cc) {
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
          ArrayList<String> fieldList = new ArrayList<>(Arrays.asList(fields));
          if (field2remove != -1) {
            fieldList.remove(field2remove);
          }
          crit.field = fieldList;
          if("STRING".equals(jsonNode.get("value").getNodeType().name())){
            crit.value = jsonNode.get("value").textValue();
          }else{
            crit.value = jsonNode.get("value");
          }
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
  }

  public String selects2String() {
    StringBuilder sb = new StringBuilder();

    Set<Map.Entry<String, Select>> entries = selects.entrySet();
    Iterator<Entry<String, Select>> entry = entries.iterator();
    int count = 0;
    int size = selects.size();
    while (entry.hasNext()) {
      Map.Entry<String, Select> entry2 = entry.next();
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
      Map.Entry<String, From> entry2 = entry.next();
      From from = entry2.getValue();
      sb.append(from.snippet).append(" ");
      if (from.asValue != null) {
        sb.append(" AS ").append(from.asValue).append(" ");
      }
      if (++count < size) {
        sb.append(", ");
      }
    }
    return sb.toString();
  }

  public static void main(String args[]) throws Exception {

    Criteria schema = new Criteria("userdata.json");
    schema.addField("'personal'").addField("'lastName'").setOperation("=").setValue("123");
    System.out.println(schema.toString());

    schema = new Criteria("userdata.json");
    schema.addField("'active'").setOperation("=").setValue("true");
    System.out.println(schema.toString());

    schema = new Criteria();
    schema.addField("'personal'").addField("'lastName'").setOperation("=").setValue("123");
    System.out.println(schema.toString());

/*    PostgresClient.setConfigFilePath("C:\\Git\\configuration\\mod-configuration-server\\src\\main\\resources\\postgres-conf.json");
    PostgresClient.getInstance(Vertx.factory.vertx() , "myuniversity3").get("users",
      JsonObject.class, new Criterion(nb), true, reply -> {
        reply.succeeded();
      });*/

    Criterion a =  json2Criterion("[{\"field\":\"'fund_distributions'->[]->'amount'->>'sum'\",\"value\":120,\"op\":\"<\"}]");
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

    Criteria aa = new Criteria();
    aa.field.add("'rush'");
    aa.operation = "!=";
    aa.value = "true";
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
/*    a.addCriterion(c, "OR", b, "AND");
    a.addCriterion(d, "OR");
    a.addCriterion(d, "AND");
    a.addCriterion(d, "OR");*/

    //a.addCriterion(c, "OR", b);
    //a.addCriterion(c, "OR", b);
    GroupedCriterias gc = new GroupedCriterias();
    GroupedCriterias gc1 = new GroupedCriterias();
    gc1.addCriteria(c, "OR");
    gc1.addCriteria(b);
    gc1.setGroupOp("NOT");
    a.addGroupOfCriterias( gc.addCriteria(b).addCriteria(c, "OR").addCriteria(d, "AND")).addCriterion(aa).addGroupOfCriterias( gc1 );
    /*
     * Criterion bb = new Criterion(); bb.addCriterion(cc, "AND", a);
     */

    System.out.println("SELECT " + a.selects2String() + " FROM " + a.from2String() + a.toString());
  }

}
