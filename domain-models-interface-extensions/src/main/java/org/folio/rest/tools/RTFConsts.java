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

  public static final String JOB_CONF_COLLECTION    = "job_confs";
  public static final String JOBS_COLLECTION        = "jobs";
  public static final String BULKS_COLLECTION       = "bulks";

  public static final String SCHEDULE_TYPE_SCHEDULED = "SCHEDULED";
  public static final String SCHEDULE_TYPE_MANUAL    = "MANUAL";

  public static final String INTERFACE_PACKAGE      = "org.folio.rest.jaxrs.resource";
  public static final String CLIENT_GEN_PACKAGE     = "org.folio.rest.client";

  public static final String  POSSIBLE_HTTP_METHOD  = "javax.ws.rs.PUT|javax.ws.rs.POST|javax.ws.rs.DELETE|javax.ws.rs.GET|"
      + "javax.ws.rs.OPTIONS|javax.ws.rs.HEAD|javax.ws.rs.TRACE|javax.ws.rs.CONNECT|org.folio.rest.jaxrs.resource.support.PATCH";

  public static final int          VALIDATION_ERROR_HTTP_CODE      = 422;
  public static final String       VALIDATION_FIELD_ERROR          = "1";
}
