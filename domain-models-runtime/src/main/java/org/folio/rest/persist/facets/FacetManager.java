package org.folio.rest.persist.facets;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.folio.rest.persist.cql.CQLWrapper;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.Version;

/**
 * @author shale
 *
 */
public class FacetManager {

  private static Configuration cfg;

  private String table;
  private String mainQuery;
  private String where = "";
  private String limitClause = "";
  private List<FacetField> facets;
  private Map<String, Object> templateInput = new HashMap<>();

  /**
   * indicate the table name to query + facet on
   * @param onTable
   */
  public FacetManager(String onTable){
    this.table = onTable;
    if(FacetManager.cfg == null){
      //do this ONLY ONCE
      FacetManager.cfg = new Configuration(new Version(2, 3, 26));
      // Where do we load the templates from:
      cfg.setClassForTemplateLoading(FacetManager.class, "/templates");
      cfg.setDefaultEncoding("UTF-8");
      cfg.setLocale(Locale.US);
      cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    }
  }

  /**
   * should be a list of facet objects indicating the path within the json to facet on, and
   * the amount of top values for each facet to return. for example:
   * new Facet("jsonb ->>'lastUpdateDate'", 5)
   * @param facets
   */
  public void setSupportFacets(List<FacetField> facets){
    this.facets = facets;
  }

  public List<FacetField> getSupportedFacets(){
    return this.facets;
  }

  public String generateFacetQuery() throws IOException, TemplateException {

    templateInput.put("facets", this.facets);

    templateInput.put("table", this.table);

    templateInput.put("where", where);

    templateInput.put("mainQuery", this.mainQuery);

    templateInput.put("limitClause", this.limitClause);

    Template template = cfg.getTemplate("base_facet_query.ftl");

    Writer writer = new StringWriter();
    template.process(templateInput, writer);
    return writer.toString();
  }

  /**
   * pass in a list of facets, where each facet is in the form of:
   * a.b.c <- path to the facet
   * a.b.c:10 <- path to the facet with count of values to return
   * a < - path to facet - top level field in the json
   *
   * @param facets
   * @param columnName - the column where the json is stored - for example jsonb.
   * @return
   */
  public static List<FacetField> convertFacetStrings2FacetFields(List<String> facets, String columnName){
    List<FacetField> facetList = null;
    if(facets != null){
      facetList = new ArrayList<>();
      for (int i = 0; i < facets.size(); i++) {
        //move to an rmb util
        String []pathAndCount = facets.get(i).split(":");
        String []pathParts = pathAndCount[0].split("\\.");
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < pathParts.length; j++) {
          if(j == pathParts.length-1){
            sb.append("->>");
          } else{
            sb.append("->");
          }
          sb.append("'").append(pathParts[j]).append("'");
        }
        FacetField ff = new FacetField("jsonb"+sb.toString());
        if(pathAndCount.length == 1){
          //default
          ff.setTopFacets2return(5);
        }else{
          ff.setTopFacets2return(Integer.valueOf(pathAndCount[1]));
        }
        facetList.add(ff);
      }
    }
    return facetList;
  }

  public String getWhere() {
    return where;
  }

  public void setWhere(String where) {
    this.where = where;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getMainQuery() {
    return mainQuery;
  }

  public String getLimitClause() {
    return limitClause;
  }

  public void setLimitClause(String limitClause) {
    this.limitClause = limitClause;
  }

  /**
   * this is the main query that will be union'ed with the facet results
   * @param mainQuery
   */
  public void setMainQuery(String mainQuery) {
    this.mainQuery = mainQuery;
  }

  public static void main(String args[]) throws Exception {

    FacetManager fm = new FacetManager("myuniversity_new1_mod_users.users");

    List<FacetField> facets = new ArrayList<>();
    facets.add(new FacetField("jsonb ->>'lastUpdateDate'", 5));
    facets.add(new FacetField("jsonb ->'personal'->>'phone'", 5));
    facets.add(new FacetField("jsonb ->>'username'", 5));

    fm.setSupportFacets(facets);

    fm.setWhere(new CQLWrapper(new CQL2PgJSON("jsonb"), "username=jha* OR username=szeev*").toString());

    fm.setMainQuery("SELECT jsonb FROM myuniversity_new1_mod_users.users where jsonb->>'username' like 'jha%' OR jsonb->>'username' like 'szeev%'" );

    System.out.println(fm.generateFacetQuery());

  }

}
