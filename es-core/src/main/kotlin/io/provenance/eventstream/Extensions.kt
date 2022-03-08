package io.provenance.eventstream.extensions

import com.google.common.io.BaseEncoding
import cosmos.base.abci.v1beta1.Abci
import io.provenance.eventstream.stream.clients.BlockResultsResponse
import io.provenance.eventstream.stream.models.BlockEvent
import io.provenance.eventstream.stream.models.BlockResultsResponseResultEvents
import io.provenance.eventstream.stream.models.TxEvent
import io.provenance.eventstream.stream.models.extensions.hash
import io.provenance.eventstream.stream.models.extensions.sha256
import io.provenance.eventstream.stream.models.extensions.toHexString
import io.provenance.eventstream.stream.models.extensions.toTxEvent
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flatMapMerge
import org.apache.commons.lang3.StringUtils
import tendermint.types.BlockOuterClass
import tendermint.types.Types
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.Base64

// === String methods ==================================================================================================

/**
 * Remove surrounding quotation marks from a string.
 */
fun String.stripQuotes(): String = this.removeSurrounding("\"")

/**
 * Base64 decode a string. In the event of failure, the original string is returned.
 */
fun String.decodeBase64(): String =
    runCatching { Base64.getDecoder().decode(this).decodeToString() }.getOrDefault(this)

/**
 * Checks if the string contains only ASCII printable characters.
 */
fun String.isAsciiPrintable(): Boolean = StringUtils.isAsciiPrintable(this)

/**
 * Decodes a string repeatedly base64 encoded, terminating when:
 *
 * - the decoded string stops changing or
 * - the maximum number of iterations is reached
 * - or the decoded string is no longer ASCII printable
 *
 * In the event of failure, the last successfully decoded string is returned.
 */
fun String.repeatDecodeBase64(): String {
    var s: String = this.toString() // copy
    var t: String = s.decodeBase64().stripQuotes()
    repeat(10) {
        if (s == t || !t.isAsciiPrintable()) {
            return s
        }
        s = t
        t = t.decodeBase64().stripQuotes()
    }
    return s
}

/**
 * Generate an ISO8601 string from the date.
 */
fun OffsetDateTime.toISOString() = this.format(DateTimeFormatter.ISO_DATE_TIME).toString()

fun <T, R> Flow<List<T>>.doFlatMap(
    ordered: Boolean,
    concurrency: Int,
    block: (List<T>) -> Flow<R>
): Flow<R> {
    return if (ordered) {
        flatMapConcat { block(it) }
    } else {
        flatMapMerge(concurrency) { block(it) }
    }
}
// === ByteArray methods ===============================================================================================

/**
 * From apache-commons-lang.
 * @see https://github.com/apache/commons-lang/blob/0a6d8b54834410b497b63599ea4e63b8659664b9/src/main/java/org/apache/commons/lang3/StringUtils.java
 */
private object StringUtils {
    /**
     * Checks if the CharSequence contains only ASCII printable characters.
     *
     *
     * `null` will return `false`.
     * An empty CharSequence (length()=0) will return `true`.
     *
     * <pre>
     * StringUtils.isAsciiPrintable(null)     = false
     * StringUtils.isAsciiPrintable("")       = true
     * StringUtils.isAsciiPrintable(" ")      = true
     * StringUtils.isAsciiPrintable("Ceki")   = true
     * StringUtils.isAsciiPrintable("ab2c")   = true
     * StringUtils.isAsciiPrintable("!ab-c~") = true
     * StringUtils.isAsciiPrintable("\u0020") = true
     * StringUtils.isAsciiPrintable("\u0021") = true
     * StringUtils.isAsciiPrintable("\u007e") = true
     * StringUtils.isAsciiPrintable("\u007f") = false
     * StringUtils.isAsciiPrintable("Ceki G\u00fclc\u00fc") = false
     </pre> *
     *
     * @param cs the CharSequence to check, may be null
     * @return `true` if every character is in the range
     * 32 thru 126
     * @since 2.1
     * @since 3.0 Changed signature from isAsciiPrintable(String) to isAsciiPrintable(CharSequence)
     */
    fun isAsciiPrintable(cs: CharSequence?): Boolean {
        if (cs == null) {
            return false
        }
        val sz = cs.length
        for (i in 0 until sz) {
            if (!CharUtils.isAsciiPrintable(cs[i])) {
                return false
            }
        }
        return true
    }
}

/**
 * From apache-commons-lang.
 * @see https://github.com/apache/commons-lang/blob/0a6d8b54834410b497b63599ea4e63b8659664b9/src/main/java/org/apache/commons/lang3/CharUtils.java
 */
private object CharUtils {
    /**
     *
     * Checks whether the character is ASCII 7 bit printable.
     *
     * <pre>
     * CharUtils.isAsciiPrintable('a')  = true
     * CharUtils.isAsciiPrintable('A')  = true
     * CharUtils.isAsciiPrintable('3')  = true
     * CharUtils.isAsciiPrintable('-')  = true
     * CharUtils.isAsciiPrintable('\n') = false
     * CharUtils.isAsciiPrintable('') = false
     </pre> *
     *
     * @param ch  the character to check
     * @return true if between 32 and 126 inclusive
     */
    fun isAsciiPrintable(ch: Char): Boolean = ch.code in 32..126
}

fun BlockOuterClass.Block.dateTime() = this.header?.dateTime()

fun Types.Header.dateTime(): OffsetDateTime? =
    runCatching { OffsetDateTime.parse(this.time.toString(), DateTimeFormatter.ISO_DATE_TIME) }.getOrNull()

fun BlockOuterClass.Block.txHash(index: Int): String? = let {
    this.data?.txsList?.get(index)?.hash() ?: null
}

fun String.hash(): String = sha256(BaseEncoding.base64().decode(this)).toHexString()
