package org.folio.rest.tools.utils;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.stream.IntStream;
import org.folio.okapi.testing.UtilityClassTester;
import org.junit.jupiter.api.Test;

class NetworkUtilsTest {

  @Test
  void utilityClass() {
    UtilityClassTester.assertUtilityClass(NetworkUtils.class);
  }

  @Test
  void nextFreePort() throws IOException {
    int port = NetworkUtils.nextFreePort();
    try (ServerSocket socket = new ServerSocket(port)) {
      assertThat(port, is(greaterThanOrEqualTo(49152)));
      assertThat(port, is(lessThan(65535)));
    }
  }

  @Test
  void nextFreePortSecond() throws IOException {
    try (ServerSocket socket50000 = new ServerSocket(50000)) {
      assertThat(NetworkUtils.nextFreePort(2, IntStream.of(50000, 50001).iterator()), is(50001));
    }
  }

  @Test
  void nextFreePort8081() throws IOException {
    try (ServerSocket socket50000 = new ServerSocket(50000);
        ServerSocket socket50001 = new ServerSocket(50001)) {
      assertThat(NetworkUtils.nextFreePort(2, IntStream.of(50000, 50001).iterator()), is(8081));
    }
  }
}
