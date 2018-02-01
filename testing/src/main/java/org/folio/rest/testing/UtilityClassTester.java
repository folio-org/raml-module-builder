package org.folio.rest.testing;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.junit.Assert;

/**
 * Unit test tool for utility classes.<p />
 * Also helps to get 100% code coverage because it invokes the private constructor.
 */
public final class UtilityClassTester {
  private UtilityClassTester() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Assert that the private constructor throws an UnsupportedOperationException.
   * @param constructor  Constructor of a utility class
   */
  @java.lang.SuppressWarnings("squid:S1166")  // ignore "Either log or rethrow this exception"
  private static void assertInvocationException(Constructor<?> constructor) {
    try {
      constructor.setAccessible(true);

      // This invocation gives 100% code coverage for the private constructor and
      // also checks that it throws the required exception.
      constructor.newInstance();
    } catch (Exception e) {
      if (e.getCause() instanceof UnsupportedOperationException) {
        return;   // this is the required exception
      }
    }
    throw new AssertionError(
        "Private constructor of utiliy class must throw UnsupportedOperationException "
        + "to fail unintended invocation via reflection.");
  }

  /**
   * Assert that the clazz has these utility class properties:
   * Class is final, has only one constructor that is private and
   * throws UnsupportedOperationException when invoked, and all methods are static.
   * @param clazz  utility class to check
   */
  public static void assertUtilityClass(final Class<?> clazz) {
    try {
      Assert.assertTrue("class must be final", Modifier.isFinal(clazz.getModifiers()));
      Assert.assertEquals("number of constructors", 1, clazz.getDeclaredConstructors().length);
      final Constructor<?> constructor = clazz.getDeclaredConstructor();
      Assert.assertTrue("constructor must be private", Modifier.isPrivate(constructor.getModifiers()));
      Assert.assertFalse("constructor accessible", constructor.isAccessible());
      assertInvocationException(constructor);
      for (final Method method : clazz.getMethods()) {
        if (method.getDeclaringClass().equals(clazz)) {
          Assert.assertTrue("method must be static - " + method, Modifier.isStatic(method.getModifiers()));
        }
      }
    } catch (Exception e) {
      throw new InternalError(e);
    }
  }
}
