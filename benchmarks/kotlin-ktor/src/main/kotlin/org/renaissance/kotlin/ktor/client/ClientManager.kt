package org.renaissance.kotlin.ktor.client

import io.ktor.util.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlin.random.Random

/**
 * Sets up and runs a fixed pool of simulated chat clients.
 *
 * Each call to [setupClients] draws two independent random subsets the client
 * pool - one to receive a group-message task, one to receive a direct-message
 * task - each of size `fraction * numberOfClients`.
 *
 * Both subsets are drawn *without replacement*, meaning that a fraction of
 * `1.0` always selects every client exactly once. The two subsets are drawn
 * independently of each other, so a client can end up with both tasks, one,
 * or none.
 *
 * A direct message's recipient is drawn *with replacement* from the same
 * client pool, including the sender itself: messaging self is treated as an
 * ordinary direct-message chat, which keeps it well-defined even for a
 * single-client pool.
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
    }

    val groupTaskCount = (userIds.size * fractionOfClientsSendingGroupMessages).toInt()
    assignRandomTasks(clientBuilders, groupTaskCount) {
      JoinGroupAndSendMessageClientTask(availableChatIds.random(random), random)
    }

    // Picking the self as the recipient is allowed. The server handles
    // messaging self and this remains correct even in a single-user case.
    val privateTaskCount = (userIds.size * fractionOfClientsSendingPrivateMessages).toInt()
    assignRandomTasks(clientBuilders, privateTaskCount) {
      DirectMessageClientTask(userIds.random(random), random)
    }

    expectedSuccessfulTaskCount = (groupTaskCount + privateTaskCount) * numberOfRequestsPerClient

    clients = clientBuilders.map { it.build() }
  }

  private fun assignRandomTasks(builders: List<Client.Builder>, count: Int, createTask: () -> ClientTask) {
    // Draw builders without replacement to assign each at most one task.
    builders.shuffled(random).take(count).forEach { it.addTaskToRun(createTask()) }
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
