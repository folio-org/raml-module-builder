package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;

import org.folio.rest.tools.client.HttpClientFactory;
import org.folio.rest.tools.client.Response;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * @author shale
 *
 */
public class HTTPMockTest {

  @Before
  public void setUp() throws Exception {
    System.setProperty("mock.httpclient", "true");
    System.out.println(System.getProperty("mock.httpclient"));
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void test() {
    HttpClientInterface client = HttpClientFactory.getHttpClient("localhost", 8080, "zxc");
    try {
      CompletableFuture<Response> cf = client.request("auth_test2");
      System.out.println("-------------------------------------->"+cf.get().getBody());
      assertEquals("1", cf.get().getBody().getString("id"));
    } catch (Exception e) {
      assertTrue(false);
      e.printStackTrace();
    }

  }



}
