package org.renaissance.kotlin.ktor.client

import io.ktor.client.plugins.websocket.*
import io.ktor.websocket.*
import org.renaissance.kotlin.ktor.common.Message
import org.renaissance.kotlin.ktor.common.User
import org.renaissance.kotlin.ktor.common.getRandomString
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

abstract class SendMessageAndAwaitClientTask(
  private val random: Random
) : ClientTask {
  private val messageId = AtomicInteger(0)

  protected suspend fun DefaultClientWebSocketSession.sendMessageToChatAndAwait(chatId: String, user: User): Boolean {
    val chatMessage = "${messageId.getAndIncrement()}_${random.getRandomString(random.nextInt(1, MAX_MESSAGE_LENGTH))}"
    sendSerialized(Message(chatId, chatMessage))

    val expectedResponse = "[${user.userName}] $chatMessage"
    return waitForMessage {
      val receivedText = (it as? Frame.Text)?.readText() ?: return@waitForMessage false
      receivedText == expectedResponse
    }
  }
}

const val MAX_MESSAGE_LENGTH = 356
