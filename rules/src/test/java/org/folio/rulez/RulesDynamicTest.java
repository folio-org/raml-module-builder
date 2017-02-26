package org.folio.rulez;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kie.api.runtime.KieSession;

/**
 * This is a class to launch  rules.
 */
public class RulesDynamicTest {

  public static final String      RULES_DIR         = "/rules";
  public static final String      RULES_FILE_PATH   = "src/main/resources/rules/";

  private Rules rules;
  private KieSession ksession;

  @Before
  public void setup(){
    try {
      rules = new Rules(generateDummyRule());
      ksession = rules.buildSession();
    }  catch (Exception e) {
      e.printStackTrace();
      Assert.fail("can't create drools session....");
    }

  }

  @Test
  public final void checkRule() throws Exception {
    try {
      Messages message = new Messages();
      ksession.insert(message);
      ksession.fireAllRules();
      Assert.assertEquals("THIS IS A TEST", message.getMessage());
    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    }
  }

  @After
  public void tearDown(){

    if(ksession != null){
      ksession.dispose();
    }
  }

  private List<String> generateDummyRule() {
    List<String> list = new ArrayList<String>();
    String newline = System.getProperty("line.separator");
    StringBuilder sb;
    sb = new StringBuilder();
    sb.append("package rules" + newline);
    for (int i = 0; i < 5; i++) {
        sb.append("import org.folio.rulez.Messages;" + newline);
        sb.append("dialect \"java\"" + newline);
        sb.append("rule \"java rule " + i + "\"" + newline);
        sb.append("when" + newline);
        sb.append("a : Messages( a.status  == 0)" + newline);
        sb.append("then" + newline);
        sb.append("a.setMessage(\"THIS IS A TEST\");" + newline);
        //sb.append("System.out.println(a.message);" + newline);
        sb.append("end" + newline);
        //System.out.println("adding" + newline + sb.toString());
        list.add(sb.toString());
        sb = new StringBuilder();
    }
    return list;
  }
}
