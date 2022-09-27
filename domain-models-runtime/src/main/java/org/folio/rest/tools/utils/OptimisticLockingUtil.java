package org.folio.rest.tools.utils;

import java.lang.reflect.Method;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class OptimisticLockingUtil {
  public final static String DB_ALLOW_SUPPRESS_OPTIMISTIC_LOCKING = "DB_ALLOW_SUPPRESS_OPTIMISTIC_LOCKING";
  private static ZonedDateTime allowSuppressOptimistcLocking;

  static {
    configureAllowSuppressOptimisticLocking(System.getenv());
  }

  private OptimisticLockingUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class");
  }

  public static void configureAllowSuppressOptimisticLocking(Map<String,String> env) {
    String s = env.getOrDefault(DB_ALLOW_SUPPRESS_OPTIMISTIC_LOCKING, "0001-01-01T00:00:00Z");
    allowSuppressOptimistcLocking = ZonedDateTime.parse(s);
  }

  /**
   * Whether DB_ALLOW_SUPPRESS_OPTIMISTIC_LOCKING environment variable allows suppressing optimistic locking
   * at the current time.
   */
  public static boolean isSuppressingOptimisticLockingAllowed() {
    return ZonedDateTime.now().isBefore(allowSuppressOptimistcLocking);
  }

  /**
   * @return a two element array containing {@code Integer entity.getVersion()} and
   *     {@code entity.setVersion(Integer); each element can be null if not found;
   *     entity is the first non-null list element.
   */
  private static <T> Method [] getVersionMethods(List<T> list) {
    Method [] methods = new Method [2];

    Optional<T> any = list.stream().filter(Objects::nonNull).findAny();
    if (any.isEmpty()) {
      return methods;
    }

    T entity = any.get();

    // entity.getClass().getMethod("setMetadata", null)
    // is 20 times slower than this loop when not found because of throwing the exception
    for (Method method : entity.getClass().getMethods()) {
      System.out.println(method.getName() + " " + method.getReturnType().getName());
      if (method.getName().equals("getVersion") &&
          method.getReturnType().equals(java.lang.Integer.class) &&
          method.getParameterCount() == 0) {
        methods[0] = method;
      } else if (method.getName().equals("setVersion") &&
          method.getParameterCount() == 1 &&
          method.getParameters()[0].getType().equals(Integer.class)) {
        methods[1] = method;
      } else {
        continue;
      }
      if (methods[0] != null && methods[1] != null) {
        return methods;
      }
    }
    return methods;
  }

  /**
   * Set version to -1 for each T of entities.
   *
   * <p>This disables optimistic locking.
   */
  public static <T> void setVersionToMinusOne(List<T> entities)
      throws ReflectiveOperationException {

    if (entities == null) {
      return;
    }

    Method setVersion = getVersionMethods(entities)[1];

    if (setVersion == null) {
      return;
    }

    for (T entity : entities) {
      if (entity == null) {
        continue;
      }
      setVersion.invoke(entity, -1);
    }
  }

  /**
   * For each T of entities set version to null if version is -1.
   *
   * <p>This enforces optimistic locking as -1 disables optimistic locking.
   */
  public static <T> void unsetVersionIfMinusOne(List<T> entities)
      throws ReflectiveOperationException {

    if (entities == null) {
      return;
    }

    Method [] methods = getVersionMethods(entities);
    Method getVersion = methods[0];
    Method setVersion = methods[1];

    if (getVersion == null || setVersion == null) {
      return;
    }

    for (T entity : entities) {
      if (entity == null) {
        continue;
      }
      Integer version = (Integer) getVersion.invoke(entity, (Object []) null);
      if (version == null || version.intValue() != -1) {
        continue;
      }

      setVersion.invoke(entity, (Object) null);
    }
  }

  /**
   * Set version to null if version is -1.
   *
   * <p>This enforces optimistic locking as -1 disables optimistic locking.
   */
  public static <T> void unsetVersionIfMinusOne(T entity) throws ReflectiveOperationException {
    unsetVersionIfMinusOne(List.of(entity));
  }
}
