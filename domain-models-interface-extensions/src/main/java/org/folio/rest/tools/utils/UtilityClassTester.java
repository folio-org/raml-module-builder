package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class UtilityClassTester {
  private UtilityClassTester() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Assert that the private constructor throws an UnsupportedOperationException.
   * @param constructor  Constructor of a utility class
   */
  private static void assertInvocationException(Constructor<?> constructor) {
    try {
      constructor.setAccessible(true);
      // This invocation gives 100% code coverage for the private constructor and
      // also checks that it throws the required exception.
      constructor.newInstance();
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof UnsupportedOperationException) {
        return;   // this is the required exception
      }
    } catch (Exception e) {
      throw new InternalError(e);
    }
    fail("Private constructor of utiliy class must throw UnsupportedOperationException "
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
      assertTrue("class is final", Modifier.isFinal(clazz.getModifiers()));
      assertEquals("number of constructors", 1, clazz.getDeclaredConstructors().length);
      final Constructor<?> constructor = clazz.getDeclaredConstructor();
      assertTrue("constructor is private", Modifier.isPrivate(constructor.getModifiers()));
      assertFalse("constructor accessible", constructor.isAccessible());
      assertInvocationException(constructor);
      for (final Method method : clazz.getMethods()) {
        if (method.getDeclaringClass().equals(clazz)) {
          assertTrue("method is static - " + method, Modifier.isStatic(method.getModifiers()));
        }
      }
    } catch (Exception e) {
      throw new InternalError(e);
    }
  }
}
