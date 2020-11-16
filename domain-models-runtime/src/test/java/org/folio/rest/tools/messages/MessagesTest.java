package org.folio.rest.tools.messages;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Test;

public class MessagesTest {

  private static String __(String language, MessageEnum consts) {
    return Messages.getInstance().getMessage(language, consts);
  }

  private static String __(String language, MessageEnum consts, Object... messageArguments) {
    return Messages.getInstance().getMessage(language, consts, messageArguments);
  }

  private static String __(String language, String code, Object... messageArguments) {
    return Messages.getInstance().getMessage(language, code, messageArguments);
  }

  @Before
  public void before() {
    Messages.getInstance().loadAllMessages();
  }

  @Test
  public void getMessageWithUmlauts() {
    MessageConsts c = MessageConsts.OperationNotSupported;
    String s = c.getCode();
    assertEquals("Operation not supported",          __("__", c));
    assertEquals("Operation not supported",          __("__", c));
    assertEquals("Operation wird nicht unterstützt", __("de", c));
    assertEquals("Operation wird nicht unterstützt", __("de", s));
  }

  @Test
  public void getMessageWithArguments() {
    MessageConsts c = MessageConsts.ContentTypeError;
    String s = c.getCode();
    assertEquals("Content-type header must be a but it is \"b\"",      __("en", c, "a", "b"));
    assertEquals("Content-type header must be a but it is \"b\"",      __("__", c, "a", "b"));
    assertEquals("Content-type header must be a but it is \"b\"",      __(null, c, "a", "b"));
    assertEquals("Content-type Header muss a sein, aber er ist \"b\"", __("de", c, "a", "b"));
    assertEquals("Content-type Header muss a sein, aber er ist \"b\"", __("de", s, "a", "b"));
    assertEquals("Error message not found: de x",                      __("de", "x", "a", "b"));
  }

  @Test
  public void textNotDefined() {
    Messages messages = Messages.getInstance();
    messages.messageMap.clear();
    MessageConsts c = MessageConsts.ContentTypeError;
    assertEquals("Error message not found: de " + c.getCode(), messages.getMessage("de", c, "a", "b"));
  }

  @Test
  public void umlaut() {
    assertEquals("Operation wird nicht unterstützt", __("de", MessageConsts.OperationNotSupported));
  }

  class MyMessages extends Messages {
    @Override
    protected void loadMessages(Path messagePath) throws IOException {
      throw new IOException("MyMessages");
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void illegalPath() {
    new MyMessages().loadMessages(MyMessages.INFRA_MESSAGES_DIR);
  }
}
