package org.folio.rulez;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.AgendaFilter;
import org.kie.api.runtime.rule.Match;

/**
 * This is a class to launch  rules.
 */
public class RulesTest {

  public static final String      RULES_DIR         = "/rules";
  public static final String      RULES_FILE_PATH   = "src/main/resources/rules/";
  
  private KieSession ksession;
  
  @Before
  public void setup(){
    try {
      ksession = new Rules().buildSession(RulesTest.class.getResource(RULES_DIR).toURI());
    }  catch (Exception e) {
      e.printStackTrace();
      Assert.fail("can't create drools session....");
    }

  }
  
  @Test
  public final void checkRule() throws Exception {
    try {
      final Messages message = new Messages();
      message.setMessage("Hello World");
      message.setStatus(Messages.HELLO);
      ksession.insert(message);
      ksession.fireAllRules();
      Assert.assertEquals(5, message.getStatus());
    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    }
  }
  
  @Test
  public final void check2ObjectsRule() throws Exception {
    try {
      final Messages1 message = new Messages1();
      message.setMessage("Hello World");
      message.setStatus(0);
      final Messages2 message2 = new Messages2();
      message2.setMessage("Hello World");
      message2.setStatus(0);
      ksession.insert(message);
      ksession.insert(message2);
      ksession.fireAllRules();
      Assert.assertEquals(5, message.getStatus());
      Assert.assertEquals(5, message2.getStatus());

    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    }
  }
  
  
  @Test
  public final void checkRuleWithAgenda() throws Exception {
    final Messages message = new Messages();
    message.setMessage("Hello World");
    message.setStatus(Messages.HELLO);
    ksession.insert(message);
    ksession.fireAllRules(new AgendaFilter() {        
      @Override
      public boolean accept(Match match) {
        if("mvel Third Rule".equals(match.getRule().getName())){
          return true;
        }
        return false;
      }
    });
    Assert.assertEquals("Whatever", message.getMessage());
  }
  
  @After
  public void tearDown(){
    
    if(ksession != null){
      ksession.dispose();
    }
  }

}
