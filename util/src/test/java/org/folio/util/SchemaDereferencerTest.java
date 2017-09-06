package org.folio.util;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.Deque;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class SchemaDereferencerTest {
  private String basePath = "src/test/resources/schemas";
  private ByteArrayOutputStream myStdErrBytes = new ByteArrayOutputStream();
  private PrintStream myStdErr = new PrintStream(myStdErrBytes);
  private PrintStream oldStdErr = null;

  private void enableMyStdErr() {
    if (oldStdErr == null) {
      oldStdErr = System.err;
    }
    System.setErr(myStdErr);
    myStdErrBytes.reset();
  }

  @After
  public void disableMyStdErr() {
    if (oldStdErr == null) {
      return;
    }
    System.setErr(oldStdErr);
    oldStdErr = null;
  }

  private SchemaDereferencer schemaDereferencer() {
    return new SchemaDereferencer(basePath);
  }

  private JsonObject readJson(String file) throws IOException {
    try (InputStream reader = new FileInputStream(file)) {
      return new JsonObject(IOUtil.toUTF8String(reader));
    }
  }

  @Test
  @Parameters({"parameters.schema, parameters.schema",
               "message.schema, message.schema.deref",
               "messages.schema, messages.schema.deref"
              })
  public void deref(String inputFile, String expectedFile) throws IOException {
    JsonObject o = schemaDereferencer().dereferencedSchema(inputFile);
    assertThat(o, is(readJson(basePath + "/" + expectedFile)));
  }

  @Test
  public void loop() throws IOException {
    try {
      schemaDereferencer().dereferencedSchema("loop.schema");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), is(endsWith("loop: loop.schema, loop.schema")));
    }
  }

  @Test
  public void loop1() throws IOException {
    try {
      schemaDereferencer().dereferencedSchema("loop1.schema");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), is(endsWith("loop: loop1.schema, loop.schema, loop.schema")));
    }
  }

  @Test
  public void loopA() throws IOException {
    try {
      schemaDereferencer().dereferencedSchema("loop_a.schema");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), is(endsWith("loop: loop_a.schema, loop_b.schema, loop_a.schema")));
    }
  }

  @Test
  public void invalidInputPath() {
    try {
      new SchemaDereferencer("::\0").dereferencedSchema("::\0");
      fail();
    } catch (IOException e) {
      e.printStackTrace(System.err);
      assertThat(e.getMessage(), containsString("::\0"));
    }
  }

  @Test
  public void cache() throws IOException {
    SchemaDereferencer dereferencer = new SchemaDereferencer(basePath) {
      private Deque<String> stack = new ArrayDeque<>();

      @Override
      public JsonObject dereferencedSchema(final String file) throws IOException {
        Deque<String> oldStack = stack;
        stack = null;
        return dereferencedSchema(file, oldStack);
      }
    };

    // read the file
    dereferencer.dereferencedSchema("parameters.schema");
    // use the cache (if it does not then NullPointerException)
    dereferencer.dereferencedSchema("parameters.schema");
    try {
      // prove that not reading from cache causes NullPointerException
      dereferencer.dereferencedSchema("message.schema");
      fail();
    } catch (NullPointerException e) {
      // null stack
    }
  }

  @Test
  public void main0() throws IOException {
    enableMyStdErr();
    SchemaDereferencer.main(new String [] {});
    assertThat(myStdErrBytes.toString(), containsString("arguments expected"));
  }

  @Test
  public void main1() throws IOException {
    enableMyStdErr();
    SchemaDereferencer.main(new String [] { "foo.schema" });
    assertThat(myStdErrBytes.toString(), containsString("is odd: 1"));
  }

  @Test
  public void main2() throws IOException {
    String destination = "target/main2.schema.deref";
    SchemaDereferencer.main(new String [] { "src/test/resources/schemas/message.schema", destination });
    assertThat(readJson(destination), is(readJson(basePath + "/message.schema.deref")));
  }
}
