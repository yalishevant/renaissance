package org.renaissance.kotlin.ktor.server

import io.ktor.util.*
import io.ktor.websocket.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import org.renaissance.kotlin.ktor.common.Chat
import org.renaissance.kotlin.ktor.common.DirectMessageChat
import org.renaissance.kotlin.ktor.common.Message
import org.renaissance.kotlin.ktor.common.User
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

const val MAX_MESSAGE_HISTORY_LENGTH = 100
const val MAX_USERNAME_LENGTH = 50

/**
 * This class is in charge of the chat server logic.
 * It contains handlers for events and commands to send messages to specific users on the server.
 */
class ChatServer(private val numberOfChatsToSetup: Int) {
  /**
   * The atomic counter used to get unique usernames based on the maximum users the server had.
   */
  private val usersCounter = AtomicInteger()

  /**
   * A concurrent map associating session IDs to usernames.
   */
  private val users = ConcurrentHashMap<String, User>()
  val chats = ConcurrentHashMap<String, Chat>()

  /**
   * Associates a session ID to a set of websockets.
   * Since a browser is able to open several tabs and windows with the same cookies and thus the same session.
   * There might be several opened sockets for the same client.
   */
  private val userSockets = ConcurrentHashMap<String, MutableList<WebSocketSession>>()

  fun setupChats() {
    chats.clear()
    for (i in 0..<numberOfChatsToSetup) {
      val id = generateNonce()
      chats[id] = Chat(id)
    }
  }

  /**
   * Handles that a member is identified by a session ID and a socket joined.
   */
  fun registerUser(userId: String, socket: WebSocketSession) {
    // Checks if this user is already registered in the server and gives him/her a temporary name if required.
    users.computeIfAbsent(userId) {
      val user = User(userId)
      user.userName = "user${usersCounter.incrementAndGet()}"
      user
    }

    // Associates this socket to the member ID.
    // Since iteration is likely to happen more frequently than adding new items,
    // we use a `CopyOnWriteArrayList`.
    val list = userSockets.computeIfAbsent(userId) { CopyOnWriteArrayList() }
    list.add(socket)
  }

  suspend fun joinChat(chatId: String, userId: String) {
    val chat = chats[chatId]!!
    val user = users[userId]!!
    // Only when joining the first socket for a member notifies the rest of the users.
    if (chat.users.add(user) && userSockets[userId]?.size == 1) {
      chats[chatId]?.broadcast("Member joined: ${user.userName}")
      userSockets[userId]?.forEach {
        it.send("You've joined chat $chatId")
        it.send("Last messages:")
        chat.lastMessages.forEach { message ->
          it.send(message)
        }
      }
    }
  }

  /**
   * Handles a [userId] identified by its session ID renaming [newName] a specific name.
   */
  suspend fun renameUser(userId: String, newName: String) {
    if (newName.isBlank())
      return sendTo(userId, "server::help", "/user [newName]")

    if (newName.length > MAX_USERNAME_LENGTH)
      return sendTo(userId, "server::help", "new name is too long: 50 characters limit")

    // Re-sets the member name.
    val user = users[userId] ?: return
    val oldName = user.userName
    synchronized(user) {
      // if userName was updated before we got the lock
      if (user.userName != oldName) return
      user.userName = newName
    }
    // Notifies everyone in the server about this change.
    userSockets[userId]?.forEach {
      it.send("You've been successfully renamed from $oldName to $newName")
    }
  }

  /**
   * Handles that a [userId] with a specific [socket] left the server.
   */
  suspend fun disconnectUserSocket(userId: String, socket: WebSocketSession) {
    // Removes the socket connection for this member
    val connections = userSockets[userId] ?: return
    connections.remove(socket)
    if (connections.isEmpty()) {
      // If there are no more connections for this member, we remove it from the server.
      userSockets.remove(userId) ?: return
      val user = users.remove(userId) ?: return

      usersCounter.decrementAndGet()

      // If the member was in a chat, we notify the rest of the members about this event.
      user.let { userInChat ->
        chats.values.forEach { chat ->
          if (chat.users.remove(userInChat)) {
            chat.broadcast("Member left: ${userInChat.userName}")
          }
        }
      }
    }
  }

  /**
   * Handles sending to a [recipientUserId] from a [senderUserId] a [message].
   *
   * Both [recipientUserId] and [senderUserId] are identified by its session-id.
   */
  suspend fun sendTo(recipientUserId: String, senderUserId: String, message: String) {
    userSockets[recipientUserId]?.sendToEach(Frame.Text("[$senderUserId] $message"))
  }

  /**
   * Handles a [message] sent from a [userId] by notifying the rest of the users.
   */
  suspend fun sendMessage(userId: String, message: Message) {
    // Pre-format the message to be send, to prevent doing it for all the users or connected sockets.
    val userName = users[userId]?.userName ?: userId
    val formatted = "[$userName] ${message.content}"

    // Sends this pre-formatted message to all the members in the server.
    val chat = chats[message.chatId] ?: return

    chat.broadcast(formatted)

    val lastMessages = chat.lastMessages
    // Appends the message to the list of [lastMessages] and caps that collection to 100 items to prevent
    // growing too much.
    lastMessages.addLast(formatted)

    if (lastMessages.size > MAX_MESSAGE_HISTORY_LENGTH) {
      lastMessages.removeFirst()
    }
  }

  /**
   * Sends a [message] to all the members in the server, including all the connections per member.
   */
  private suspend fun Chat.broadcast(message: String) {
    users.asSequence().mapNotNull { userSockets[it.userId] }.forEach { sockets ->
      sockets.sendToEach(Frame.Text(message))
    }
  }

  /**
   * Sends a [frame] to every socket in [this] list. Skips over sockets that are
   * already closed ([ClosedSendChannelException]) or broken ([IOException]), but
   * does not close them (it did not open them). Anything else (a bug, or our own
   * cancellation) is left to propagate. Cancellations need to be handled with care,
   * because we may get a [CancellationException] when a different coroutine cancels
   * the [WebSocketSession] (dropping buffered outgoing messages). That is also
   * skipped, but only if it did not come from our own coroutine being cancelled.
   */
  private suspend fun List<WebSocketSession>.sendToEach(frame: Frame) {
    forEach { session ->
      try {
        session.send(frame.copy())
      } catch (_: ClosedSendChannelException) {
        // already gone; nothing to do here
      } catch (_: IOException) {
        // transport broken; nothing to do here
      } catch (t: CancellationException) {
        if (!currentCoroutineContext().isActive) throw t
        // not our cancellation; nothing to do here
      }
    }
  }

  fun createChat(creatorUserId: String): String {
    val chat = Chat(generateNonce())
    chats[chat.id] = chat

    chat.users.add(users[creatorUserId]!!)
    return chat.id
  }

  fun createDirectMessageChat(creatorUserId: String, inviteeUserId: String): String {
    val chat = chats.computeIfAbsent(DirectMessageChat.hashForChat(creatorUserId, inviteeUserId)) {
      DirectMessageChat(
        creatorUserId,
        inviteeUserId,
        it
      ).apply {
        users.add(this@ChatServer.users[creatorUserId]!!)
        users.add(this@ChatServer.users.computeIfAbsent(inviteeUserId) {
          User(inviteeUserId).apply {
            userName = "user${usersCounter.incrementAndGet()}"
          }
        })
      }
    }
    return chat.id
  }
}
