package org.folio.rest.tools.utils;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.BeforeClass;
import org.junit.Test;

public class OutStreamTest {
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

  @Test
  public void utf8() throws IOException {
    OutStream s = new OutStream();
    s.setData("ä");
    assertEquals("ä", s.getData());
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    s.write(result);
    assertArrayEquals("ä".getBytes(StandardCharsets.UTF_8), result.toByteArray());
  }

}
