package org.folio.rest.tools.utils;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class OutStreamTest {
  private Charset oldDefaultCharset;

  private static int major;

  @BeforeClass
  public static void beforeClass() {
    String version = System.getProperty("java.version");
    int dot = version.indexOf('.');
    major = Integer.parseInt(version.substring(0, dot));
  }

  private void setDefaultCharset(Charset charset) {
    Assume.assumeTrue(major < 17);
    if (oldDefaultCharset == null) {
      oldDefaultCharset = Charset.defaultCharset();
    }
    try {
      // default charset is cached by Charset, use reflection to change it
      Class<Charset> clazz = Charset.class;
      Field field = clazz.getDeclaredField("defaultCharset");
      field.setAccessible(true);
      field.set(null, charset);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @After
  public void tearDown() {
    if (oldDefaultCharset == null) {
      return;
    }
    // defaultCharset has been changed, restore it
    setDefaultCharset(oldDefaultCharset);
  }

  @Test
  public void getWithoutSet() {
    assertNull(new OutStream().getData());
  }

  @Test
  public void setGet() {
    OutStream s = new OutStream();
    s.setData("ab");
    assertEquals("ab", s.getData());
  }

  @Test
  public void setGet2() {
    OutStream s = new OutStream();
    s.setData("ab");
    s.setData("cd");
    assertEquals("cd", s.getData());
  }

  @Test
  public void setGetNull() {
    OutStream s = new OutStream();
    s.setData("ab");
    s.setData(null);
    assertNull(s.getData());
  }

  @Test(expected = NullPointerException.class)
  public void writeNull() throws IOException {
    new OutStream().write(new ByteArrayOutputStream());
  }

  private void testBytes() {
    try {
      OutStream s = new OutStream();
      s.setData("ä");
      ByteArrayOutputStream result = new ByteArrayOutputStream();
      s.write(result);
      assertArrayEquals("ä".getBytes(StandardCharsets.UTF_8), result.toByteArray());
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void defaultCharset() {
    testBytes();
  }

  @Test
  public void utf8() {
    setDefaultCharset(StandardCharsets.UTF_8);
    testBytes();
  }

  @Test
  public void usAscii() {
    setDefaultCharset(StandardCharsets.US_ASCII);
    testBytes();
  }

  @Test
  public void iso_8859_1() throws IOException {
    setDefaultCharset(StandardCharsets.ISO_8859_1);
    testBytes();
  }
}
