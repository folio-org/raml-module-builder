package org.folio.rest.tools.utils;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.folio.okapi.common.OkapiToken;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.Metadata;

public final class MetadataUtil {
  private MetadataUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class");
  }

  /**
   * @return entity.setMetadata(Metadata) method, or null if not found.
   */
  private static Method getSetMetadataMethod(Object entity) {
    // entity.getClass().getMethod("setMetadata", new Class[] { Metadata.class })
    // is 20 times slower than this loop when not found because of throwing the exception
    for (Method method : entity.getClass().getMethods()) {
      if (method.getName().equals("setMetadata") &&
          method.getParameterCount() == 1 &&
          method.getParameters()[0].getType().equals(Metadata.class)) {
        return method;
      }
    }
    return null;
  }

  /**
   * @return Metadata where createdDate and updatedDate are set to current time and
   * createdByUserId and updatedByUserId are set to the
   * {@link RestVerticle#OKAPI_USERID_HEADER} header, using {@code user_id} from the
   * {@link RestVerticle#OKAPI_HEADER_TOKEN} as a fall-back. The token is used without
   * validation.
   */
  public static Metadata createMetadata(Map<String, String> okapiHeaders) {
    String userId = okapiHeaders.get(RestVerticle.OKAPI_USERID_HEADER);
    if (userId == null) {
      try {
        userId = new OkapiToken(okapiHeaders.get(RestVerticle.OKAPI_HEADER_TOKEN)).getUserIdWithoutValidation();
      } catch (Exception e) {
        // ignore, userId remains null
      }
    }
    Metadata md = new Metadata();
    md.setUpdatedDate(new Date());
    md.setCreatedDate(md.getUpdatedDate());
    md.setCreatedByUserId(userId);
    md.setUpdatedByUserId(userId);
    return md;
  }

  /**
   * Populate the metadata of entity. Do nothing if entity doesn't have a setMetadata method.
   */
  public static void populateMetadata(Object entity, Map<String, String> okapiHeaders)
      throws ReflectiveOperationException {
    //try to populate meta data section of the passed in json (converted to pojo already as this stage)
    //will only succeed if the pojo (json schema) has a reference to the metaData schema.
    //there should not be a metadata schema declared in the json schema unless it is the OOTB meta data schema.
    // The createdDate and createdByUserId fields are stored in the db in separate columns on insert trigger so that even if
    // we overwrite them here, the correct value will be reset via a database trigger on update.
    populateMetadata(Collections.singletonList(entity), okapiHeaders);
  }

  /**
   * Populate each T of entities with the same instance of Metadata based on the userId value taken
   * from user id header or token header of okapiHeaders.
   */
  public static <T> void populateMetadata(List<T> entities, Map<String, String> okapiHeaders)
      throws ReflectiveOperationException {

    if (entities == null) {
      return;
    }

    Method setMetadata = null;
    Metadata metadata = null;
    for (T entity : entities) {
      if (entity == null) {
        continue;
      }
      if (metadata == null) {
        setMetadata = getSetMetadataMethod(entity);
        if (setMetadata == null) {
          return;
        }
        metadata = createMetadata(okapiHeaders);
      }
      setMetadata.invoke(entity, metadata);
    }
  }
}
