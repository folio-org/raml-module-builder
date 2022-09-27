package org.folio.rest.tools.utils;

import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.folio.okapi.testing.UtilityClassTester;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OptimisticLockingUtilTest {

  @BeforeEach
  void beforeEach() {
    OptimisticLockingUtil.configureAllowSuppressOptimisticLocking(Map.of());
  }

  @Test
  void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(OptimisticLockingUtil.class);
  }

  @Test
  void setVersionToMinusOne_null() throws ReflectiveOperationException {
    OptimisticLockingUtil.setVersionToMinusOne(null);
  }

  @Test
  void setVersionToMinusOne_empty() throws ReflectiveOperationException {
    OptimisticLockingUtil.setVersionToMinusOne(List.of());
  }

  @Test
  void setVersionToMinusOne_one() throws ReflectiveOperationException {
    String uuid = UUID.randomUUID().toString();
    User user = new User().withId(uuid);
    OptimisticLockingUtil.setVersionToMinusOne(List.of(user));
    assertThat(user.getVersion(), is(-1));
    assertThat(user.getId(), is(uuid));
  }

  @Test
  void setVersionToMinusOne_three() throws ReflectiveOperationException {
    String uuid1 = UUID.randomUUID().toString();
    String uuid2 = UUID.randomUUID().toString();
    String uuid3 = UUID.randomUUID().toString();
    User user1 = new User().withId(uuid1).withVersion(0);
    User user2 = new User().withId(uuid2).withVersion(Integer.MIN_VALUE);
    User user3 = new User().withId(uuid3).withVersion(Integer.MAX_VALUE);
    OptimisticLockingUtil.setVersionToMinusOne(List.of(user1, user2, user3));
    assertThat(user1.getVersion(), is(-1));
    assertThat(user2.getVersion(), is(-1));
    assertThat(user3.getVersion(), is(-1));
    assertThat(user1.getId(), is(uuid1));
    assertThat(user2.getId(), is(uuid2));
    assertThat(user3.getId(), is(uuid3));
  }

  class WrongMethods {
    public void setMetadata(String s) {
    }
    public void setMetadata(Metadata metadata, String x) {
    }
  }

  @Test
  void setVersionToMinusOne_wrongMethods() {
    assertDoesNotThrow(() ->
        OptimisticLockingUtil.setVersionToMinusOne(List.of(new WrongMethods())));
  }

  @Test
  void unsetVersionIfMinusOne_null() throws ReflectiveOperationException {
    OptimisticLockingUtil.unsetVersionIfMinusOne(null);
  }

  @Test
  void unsetVersionIfMinusOne_empty() throws ReflectiveOperationException {
    OptimisticLockingUtil.unsetVersionIfMinusOne(List.of());
  }

  @Test
  void unsetVersionIfMinusOne_one() throws ReflectiveOperationException {
    String uuid = UUID.randomUUID().toString();
    User user = new User().withId(uuid).withVersion(-1);
    OptimisticLockingUtil.unsetVersionIfMinusOne(List.of(user));
    assertThat(user.getVersion(), is(nullValue()));
    assertThat(user.getId(), is(uuid));
  }

  @Test
  void unsetVersionIfMinusOne_six() throws ReflectiveOperationException {
    String uuid1 = UUID.randomUUID().toString();
    String uuid2 = UUID.randomUUID().toString();
    String uuid3 = UUID.randomUUID().toString();
    String uuid4 = UUID.randomUUID().toString();
    String uuid5 = UUID.randomUUID().toString();
    String uuid6 = UUID.randomUUID().toString();
    User user1 = new User().withId(uuid1).withVersion(-1);
    User user2 = new User().withId(uuid2).withVersion(Integer.MIN_VALUE);
    User user3 = new User().withId(uuid3).withVersion(0);
    User user4 = new User().withId(uuid4).withVersion(Integer.MAX_VALUE);
    User user5 = new User().withId(uuid5);
    User user6 = new User().withId(uuid6).withVersion(-1);
    OptimisticLockingUtil.unsetVersionIfMinusOne(List.of(user1, user2, user3, user4, user5, user6));
    assertThat(user1.getVersion(), is(nullValue()));
    assertThat(user2.getVersion(), is(Integer.MIN_VALUE));
    assertThat(user3.getVersion(), is(0));
    assertThat(user4.getVersion(), is(Integer.MAX_VALUE));
    assertThat(user5.getVersion(), is(nullValue()));
    assertThat(user6.getVersion(), is(nullValue()));
    assertThat(user1.getId(), is(uuid1));
    assertThat(user2.getId(), is(uuid2));
    assertThat(user3.getId(), is(uuid3));
    assertThat(user4.getId(), is(uuid4));
    assertThat(user5.getId(), is(uuid5));
    assertThat(user6.getId(), is(uuid6));
  }

  @Test
  void unsetVersionIfMinusOne_wrongMethods() {
    assertDoesNotThrow(() ->
        OptimisticLockingUtil.unsetVersionIfMinusOne(List.of(new WrongMethods())));
  }

  @Test
  void isSuppressAllowed_default() {
    assertThat(OptimisticLockingUtil.isSuppressingOptimisticLockingAllowed(), is(false));
  }

  @Test
  void isSupporessAllowed_expired() {
    OptimisticLockingUtil.configureAllowSuppressOptimisticLocking(
        Map.of(OptimisticLockingUtil.DB_ALLOW_SUPPRESS_OPTIMISTIC_LOCKING, "2022-01-01T00:00:00Z"));
    assertThat(OptimisticLockingUtil.isSuppressingOptimisticLockingAllowed(), is(false));
  }

  @Test
  void isSupporessAllowed_future() {
    OptimisticLockingUtil.configureAllowSuppressOptimisticLocking(
        Map.of(OptimisticLockingUtil.DB_ALLOW_SUPPRESS_OPTIMISTIC_LOCKING, "2999-01-01T00:00:00Z"));
    assertThat(OptimisticLockingUtil.isSuppressingOptimisticLockingAllowed(), is(true));
  }
}
