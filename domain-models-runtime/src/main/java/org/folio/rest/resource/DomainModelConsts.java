package org.folio.rest.resource;


public class DomainModelConsts {
  public static final String PACKAGE_OF_IMPLEMENTATIONS = "org.folio.rest.impl";
  public static final String PACKAGE_OF_HOOK_INTERFACES = "org.folio.rest.resource.interfaces";
  public static final String SOURCES_DEFAULT = "ramls";
  public static final String JSON_SCHEMA_LIST = "json-schema.list";

  /** use for {@link org.folio.rest.jaxrs.model.Error#setType(String)} */
  public static final String VALIDATION_FIELD_ERROR = "1";

  public static final String  PATH_PARAM             = "@javax.ws.rs.PathParam";
  public static final String  HEADER_PARAM           = "@javax.ws.rs.HeaderParam";
  public static final String  QUERY_PARAM            = "@javax.ws.rs.QueryParam";
  public static final String  DEFAULT_PARAM          = "@javax.ws.rs.DefaultValue";
}
