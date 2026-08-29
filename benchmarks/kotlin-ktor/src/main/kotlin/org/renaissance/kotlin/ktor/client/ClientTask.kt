package org.renaissance.kotlin.ktor.client

import io.ktor.client.plugins.websocket.*

interface ClientTask {
  /**
   * Primes this task's internal state (if it has any).
   * Called once before repeated calls to [run].
   */
  suspend fun setup(session: DefaultClientWebSocketSession) {}

  /**
   * Runs this task. Called repeatedly after initial call to [setup].
   *
   * @return `true` if the task completed and the server responded as
   *   expected,`false` if it completed without the expected response.
   * @throws Exception if something is broken rather than unvalidated.
   */
  suspend fun run(session: DefaultClientWebSocketSession): Boolean = true
}
