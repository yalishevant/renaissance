package org.renaissance.kotlin.ktor.common

/**
 * Represents a user in the chat system.
 *
 * @property id The unique identifier for the user.
 * @property name The display name of the user.
 */
data class User(val id: String) {
  var name: String = id
}
