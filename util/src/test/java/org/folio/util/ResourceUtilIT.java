package org.folio.util;

/**
 * Integration tests (IT) use a jar file while unit tests use the classes directory.
 * Only for loading a resource from a jar file the leading slash must be removed.
 */
class ResourceUtilIT extends ResourceUtilTest {
  // Same tests from ResourceUtilTest but running them against a jar file.
}
