package org.renaissance.kotlin.ktor.client

import io.ktor.util.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlin.random.Random

/**
 * A **probabilistic** client manager, which sets up the appropriate number of clients, performing a specified number and kind of tasks
 */
class ClientManager(
  private val port: Int,
  numberOfClients: Int,
  private val numberOfRequestsPerClient: Int,
  private val fractionOfClientsSendingGroupMessages: Double,
  private val fractionOfClientsSendingPrivateMessages: Double,
  randomSeed: Int,
  private val coroutineScope: CoroutineScope
) {
  private val random = Random(randomSeed)

  // ArrayList is desired for List.random().
  private val userIds: ArrayList<String> = ArrayList(
    random.generateUserIds(numberOfClients)
  )

  private var clients: List<Client> = emptyList()

  /**
   * How many client task executions the current [clients] should complete
   * successfully in total, across all repetitions - the number [setupClients]
   * actually assigned, not a value re-derived from raw parameters elsewhere.
   */
  var expectedSuccessfulTaskCount: Int = 0
    private set

  private fun Random.generateUserIds(count: Int): Set<String> =
    generateSequence { generateNonce() }.distinct().take(count).toSet()

  fun setupClients(availableChatIds: List<String>) {
    val clientBuilders = userIds.map { userId ->
      Client.Builder(port, userId, numberOfRequestsPerClient, createDefaultClient())
    }.toMutableList()

    val groupTaskCount = (userIds.size * fractionOfClientsSendingGroupMessages).toInt()
    clientBuilders.take(groupTaskCount).forEach { builder ->
      builder.addTaskToRun(JoinGroupAndSendMessageClientTask(availableChatIds.random(random), random))
    }
    clientBuilders.shuffle(random)

    // Picking the self as the recipient is allowed. The server handles
    // messaging self and this remains correct even in a single-user case.
    val privateTaskCount = (userIds.size * fractionOfClientsSendingPrivateMessages).toInt()
    clientBuilders.take(privateTaskCount).forEach { builder ->
      builder.addTaskToRun(DirectMessageClientTask(userIds.random(random), random))
    }
    clientBuilders.shuffle(random)

    expectedSuccessfulTaskCount = (groupTaskCount + privateTaskCount) * numberOfRequestsPerClient

    clients = clientBuilders.map { it.build() }
  }

  suspend fun runClients(): Int {
    return clients.map { coroutineScope.async(start = CoroutineStart.LAZY) { it.run() } }
      .map { it.also { it.start() } }
      .awaitAll()
      .sum()
  }

  fun closeClients() {
    clients.forEach { it.close() }
    clients = emptyList()
  }

}
