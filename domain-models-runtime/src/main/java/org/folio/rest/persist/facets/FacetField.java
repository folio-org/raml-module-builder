package org.folio.rest.persist.facets;

/**
 * @author shale
 *
 */
public class FacetField {

  private String fieldPath;
  private int topFacets2return;
  private  String alias;

  public FacetField(String path2facet){
    this(path2facet, 5, null);
  }

  public FacetField(String path2facet, int amountOfValues2return){
    this(path2facet, amountOfValues2return, null);
  }

  private FacetField(String path2facet, int topFacets2return, String alias){
    this.fieldPath = path2facet;
    this.topFacets2return = topFacets2return;
    if(path2facet == null || !path2facet.contains("\'")){
      throw new ExceptionInInitializerError("Path to facet must be in the form of jsonb_field_name->'field1'->>'field2', for example jsonb->>'field'");
    }
    //alias is the last field name wrapped in ''
    this.alias = path2facet.trim().replaceAll(".*->>'|'$", "");
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
