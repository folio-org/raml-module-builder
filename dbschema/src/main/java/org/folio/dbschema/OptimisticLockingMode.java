package org.folio.dbschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum OptimisticLockingMode {

  @JsonProperty("off")
  OFF,

  @JsonProperty("logOnConflict")
  LOG,

  @JsonProperty("failOnConflict")
  FAIL,

  @JsonProperty("failOnConflictUnlessSuppressed")
  FAIL_SUPPRESS

}
