package org.folio.rest.persist.facets;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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

  private static int calculateOnFirst = 10000; //calculate facets on first N records in result set

  private static Configuration cfg;

  private String table;
  private String mainQuery;
  private String where = "";
  private String limitClause = ""; // limit of results - should be extracted from original query
  private String offsetClause = ""; // offset of results - should be extracted from original query
  private String idField ="";
  private String schema =""; // tenantid_module
  private String countQuery = "";
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
      cfg.setClassForTemplateLoading(FacetManager.class, "/templates/facets");
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

    templateInput.put("offsetClause", this.offsetClause);

    templateInput.put("idField", idField);

    templateInput.put("schema", this.schema);

    templateInput.put("countQuery", this.countQuery.replace("'", "''"));

    templateInput.put("calculateOnFirst" , calculateOnFirst+"");

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
        String facetPath = facets.get(i);
        boolean isFacetOnArrayField = false;
        if(facetPath.contains("[]")){
          isFacetOnArrayField= true;
        }
        String []pathAndCount = facetPath.split(":");
        String []pathParts = pathAndCount[0].split("\\.");
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < pathParts.length; j++) {
          if(j==0){
            //always start the paths with the column name, only once at the beginning
            sb.append("jsonb");
          }
          if(pathParts[j].endsWith("[]")){
            //build a path ->... once you reach a [] wrap the path in a jsonb_array_elements
            String fieldName = pathParts[j].substring(0,  pathParts[j].length()-2);
            String soFar = sb.toString()+"->'"+fieldName +"'";
            sb = new StringBuilder( buildPathForArray(soFar) );
          }
          else{
            if(j == pathParts.length-1 && !isFacetOnArrayField){
              sb.append("->>");
            } else{
              sb.append("->");
            }
            sb.append("'").append(pathParts[j]).append("'");
          }
        }
        FacetField ff = new FacetField(sb.toString());
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

  private static String buildPathForArray(String path) {
    return "(jsonb_array_elements(("+ path + "))::jsonb)";
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

  public String getIdField() {
    return idField;
  }

  public void setIdField(String idField) {
    this.idField = idField;
  }

  public String getOffsetClause() {
    return offsetClause;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public void setOffsetClause(String offsetClause) {
    this.offsetClause = offsetClause;
  }

  public static int getCalculateOnFirst() {
    return calculateOnFirst;
  }

  public static void setCalculateOnFirst(int firstN) {
    calculateOnFirst = firstN;
  }

  public String getCountQuery() {
    return countQuery;
  }

  public void setCountQuery(String countQuery) {
    this.countQuery = countQuery;
  }
}
