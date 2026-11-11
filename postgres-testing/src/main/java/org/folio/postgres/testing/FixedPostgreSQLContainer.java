package org.folio.postgres.testing;

import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * @deprecated Will be removed as soon as upstream releases a fixed PostgreSQLContainer
 */
@Deprecated
public class FixedPostgreSQLContainer extends PostgreSQLContainer {

  /**
   * @deprecated Will be removed as soon as upstream releases a fixed PostgreSQLContainer
   */
  @Deprecated
  public FixedPostgreSQLContainer(final String dockerImageName) {
    super(dockerImageName);
  }

  /**
   * @deprecated Will be removed as soon as upstream releases a fixed PostgreSQLContainer
   */
  @Deprecated
  @Override
  protected void waitUntilContainerStarted() {
    // fix race condition until upstream releases a fix:
    // https://github.com/testcontainers/testcontainers-java/pull/12080
    getWaitStrategy().waitUntilReady(this);
    super.waitUntilContainerStarted();
  }
}
