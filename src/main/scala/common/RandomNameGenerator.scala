package common

import org.apache.commons.text.RandomStringGenerator

/** Generates a random string of given length, with characters in the range [A-Z,a-z]. */
object RandomNameGenerator {

  private val randomStringLength: Int = 5

  private val randomStringGenerator: RandomStringGenerator =
    new RandomStringGenerator.Builder().selectFrom(('A' to 'Z') ++ ('a' to 'z'): _*).build()

  def randomString(length: Int = randomStringLength): String = {
    s"${randomStringGenerator.generate(length)}"
  }
}
