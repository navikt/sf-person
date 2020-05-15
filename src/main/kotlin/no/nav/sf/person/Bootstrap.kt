package no.nav.sf.person

import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

object Bootstrap {

    private val log = KotlinLogging.logger { }

    fun start(ev: EnvVar = EnvVar()) {
        log.info { "Starting" }
        ShutdownHook.reset()
        enableNAISAPI {
            ServerState.reset()
            loop(Params(envVar = ev))
        }
        log.info { "Finished!" }
    }

    private tailrec fun loop(p: Params) {

        log.info { "Get parameters from dynamic vault and static env. variables" }
        val newP = Params(
                vault = Vault(),
                prevVault = p.vault,
                envVar = p.envVar,
                sfAuthorization = p.sfAuthorization
        )

        val newAuthorization: AuthorizationBase = when (val i = p.integrityCheck()) {
            is IntegrityIssue -> {
                log.error { i.cause }
                ServerState.flag(ServerStates.IntegrityIssues)
                // flag need for attention
                // TODO - fix misuse of metric giving alert on slack
                Metrics.failedRequest.inc()
                AuthorizationMissing
            }
            is IntegrityOk -> {
                log.info { "Proxy details: ${p.envVar.httpsProxy}" }

                // some resets before next attempt/work session
                Metrics.sessionReset()

                work(newP).also {
                    if (!ShutdownHook.isActive() && ServerState.isOk()) conditionalWait(p.envVar.msBetweenWork)
                }
            }
        }

        if (!ShutdownHook.isActive() && ServerState.isOk()) loop(
                Params(envVar = newP.envVar, sfAuthorization = newAuthorization)
        )
    }

    private fun conditionalWait(ms: Long) = runBlocking {

        log.info { "Will wait $ms ms before starting all over" }

        val cr = launch {
            runCatching { delay(ms) }
                    .onSuccess { log.info { "waiting completed" } }
                    .onFailure { log.info { "waiting interrupted" } }
        }

        tailrec suspend fun loop(): Unit = when {
            cr.isCompleted -> Unit
            ServerState.preStopIsActive() || ShutdownHook.isActive() -> cr.cancel()
            else -> {
                delay(250L)
                loop()
            }
        }

        loop()
        cr.join()
    }
}
