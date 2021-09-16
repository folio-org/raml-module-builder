package org.folio.rest.persist.facets;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author shale
 *
 */
public class FacetField {

  private static final Pattern QUOTES_PATTERN = Pattern.compile("(\'[^\']*\')", Pattern.CASE_INSENSITIVE);

  private String fieldPath;
  private int topFacets2return;
  private  String alias;

  public FacetField(String path2facet){
    this(path2facet, 5, null);
  }

  public FacetField(String path2facet, int amountOfValues2return){
    this(path2facet, amountOfValues2return, null);
  }
  /**
  * //line:33-causing security hotspot ,@SupressWarnings can be used as input is very small."replaceAll(".*->>'|'$", "")".
  */
  @SuppressWarnings("all")
  private FacetField(String path2facet, int topFacets2return, String alias){
    this.fieldPath = path2facet;
    this.topFacets2return = topFacets2return;
    if(path2facet == null || !path2facet.contains("\'")){
      throw new ExceptionInInitializerError("Path to facet must be in the form of jsonb_field_name->'field1'->>'field2', for example jsonb->>'field'");
    }
    //alias is the last field name wrapped in ''
    this.alias = path2facet.trim().replaceAll(".*->>'|'$", "");
    if(path2facet.contains("jsonb_array_elements(")){
      //array path , get last occurrence of what is in between ''
      Matcher m = QUOTES_PATTERN.matcher(path2facet);
      while (m.find()){
        this.alias = m.group();
        //remove ''
        this.alias = this.alias.substring(1, this.alias.length()-1);
      }
    }
  }

  public String getFieldPath() {
    return fieldPath;
  }

  public void setFieldPath(String fieldPath) {
    this.fieldPath = fieldPath;
  }

  public int getTopFacets2return() {
    return topFacets2return;
  }

  public void setTopFacets2return(int topFacets2return) {
    this.topFacets2return = topFacets2return;
  }

  public String getAlias() {
    return alias;
  }


}
