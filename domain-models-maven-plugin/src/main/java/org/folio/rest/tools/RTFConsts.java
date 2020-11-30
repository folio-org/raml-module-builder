package org.folio.rest.tools;


public class RTFConsts {

  public static final String OK_PROCESSING_STATUS    = "OK_PROCESSING";
  public static final String ERROR_PROCESSING_STATUS = "ERROR_PROCESSING";

  public static final String PACKAGE_OF_IMPLEMENTATIONS      = "org.folio.rest.impl";
  public static final String PACKAGE_OF_HOOK_INTERFACES      = "org.folio.rest.resource.interfaces";

  public static final String STATUS_PENDING         = "PENDING";
  public static final String STATUS_RUNNING         = "RUNNING";
  public static final String STATUS_COMPLETED       = "COMPLETED";
  public static final String STATUS_ERROR           = "ERROR";
  public static final String STATUS_ERROR_THRESHOLD = "FAILED_ERROR_THRESHOLD";

  public static final String IMPORT_MODULE        = "IMPORTS";

  public static final String SCHEDULE_TYPE_SCHEDULED = "SCHEDULED";
  public static final String SCHEDULE_TYPE_MANUAL    = "MANUAL";

  public static final String JAXRS_PACKAGE          = "org.folio.rest.jaxrs";
  public static final String INTERFACE_PACKAGE      = "org.folio.rest.jaxrs.resource";
  public static final String CLIENT_GEN_PACKAGE     = "org.folio.rest.client";

  public static final int          VALIDATION_ERROR_HTTP_CODE      = 422;
  public static final String       VALIDATION_FIELD_ERROR          = "1";
}
