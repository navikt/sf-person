package no.nav.sf.person

/**
 * Very simple mock in test context, simulating
 * - Salesforce JWT authorization
 * - Salesforce sObject POST
 *
 * The latter will
 * - When 2nd POST return UNAUTHORIZED
 * - When 4th POST, always return UNAUTHORIZED
 */

import java.net.URL
import mu.KotlinLogging
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.routing.bind
import org.http4k.routing.routes
import org.http4k.server.Netty
import org.http4k.server.asServer

private val log = KotlinLogging.logger {}

const val MOCK_ValidClientID = "validClientID"
const val MOCK_ValidUsername = "validUsername"
const val MOCK_ValidToken = "notExpired"
const val MOCK_TokenType = "Bearer"

internal fun sfMockAPI(doSomething: () -> Unit) {

    data class MockDetails(
        val authorizationPath: String,
        val restPath: String,
        val instanceUrl: String,
        val port: Int
    )

    val md = Params().getSalesforceDetails().let { m ->
        MockDetails(
                URL(m.oAuthEndpoint()).path,
                m.sObjectPath(),
                m.url,
                URL(m.url).port
        )
    }

    var restPathCounter = 0 // ugly, but easy...

    val validAuthResponse = """
                    {"access_token":"$MOCK_ValidToken","instance_url":"${md.instanceUrl}","id":"","token_type":"$MOCK_TokenType","issued_at":"test","signature":"test"}
                """.trimIndent()

    fun Request.validClientIDAndUsername(): Boolean = runCatching {
        query("assertion")?.let { a ->
            when (val jcs = JWTClaimSet.fromJson(a.split('.')[1].decodeB64().toString(Charsets.UTF_8))) {
                is JWTClaimSetMissing -> false
                is JWTClaimSet -> jcs.iss == MOCK_ValidClientID && jcs.sub == MOCK_ValidUsername
            }
        } ?: false
    }
            .getOrDefault(false)

    fun Request.validAccessTypeAndToken(): Response = runCatching {
        header("Authorization")?.let { a ->
            when (a) {
                "$MOCK_TokenType $MOCK_ValidToken" -> Response(Status.OK).body("")
                else -> Response(Status.UNAUTHORIZED).body("")
            }
        } ?: Response(Status.UNAUTHORIZED).body("")
    }
            .getOrDefault(Response(Status.UNAUTHORIZED).body(""))

    fun restPOSTManager(r: Request, counter: Int = 1): Response = when (counter) {
        1 -> r.validAccessTypeAndToken()
        2 -> Response(Status.UNAUTHORIZED).body("")
        3 -> r.validAccessTypeAndToken()
        else -> Response(Status.UNAUTHORIZED).body("")
    }

    val api = routes(
        md.authorizationPath bind Method.POST to { r ->
                when (r.validClientIDAndUsername()) {
                    false -> Response(Status.BAD_REQUEST).body("")
                    true -> Response(Status.OK).body(validAuthResponse)
                }
        },
        md.restPath bind Method.POST to { r ->
            restPathCounter += 1
            restPOSTManager(r, restPathCounter)
        }
    )

    val srv = api.asServer(Netty(md.port))

    try {
        srv.start()
        log.info { "sfAPI started" }
        doSomething()
        srv.stop()
    } catch (e: Exception) {
        log.error { "Couldn't activate sfAPI - ${e.message}" }
    } finally {
        srv.close()
        log.info { "sfAPI stopped" }
    }
}
