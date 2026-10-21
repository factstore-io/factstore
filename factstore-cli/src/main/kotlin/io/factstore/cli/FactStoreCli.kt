package io.factstore.cli

import com.github.ajalt.clikt.core.CliktError
import com.github.ajalt.clikt.command.SuspendingCliktCommand
import com.github.ajalt.clikt.command.parse
import io.factstore.cli.command.FactStoreCommand
import io.factstore.client.exceptions.FactStoreException
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.annotations.QuarkusMain
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration.Companion.milliseconds

private const val EXIT_OK = 0
private const val EXIT_FAILURE = 1

/** What a shell reports for a process stopped by Ctrl+C: 128 + SIGINT. */
private const val EXIT_INTERRUPTED = 130

/** How long a shutdown waits for the cancelled command to finish. */
private val CANCELLATION_GRACE = 500.milliseconds

/**
 * The process: Quarkus starts the container, hands the arguments over, and exits with what
 * this returns.
 *
 * The commands are CDI beans, so the command tree is wired by injection rather than by a
 * registry of classes, and each command names the collaborators it needs in its constructor.
 */
@QuarkusMain
class FactStoreCli(
    private val factStoreCommand: FactStoreCommand,
) : QuarkusApplication {

    /** The command being run, which a shutdown cancels. */
    private val execution = Job()

    // The one place a coroutine is started: every command below is a suspending function.
    override fun run(vararg args: String): Int =
        try {
            runBlocking(execution) { factStoreCommand.execute(args.asList()) }
        } catch (e: CancellationException) {
            // Only a shutdown is the user quitting. Any other cancellation -- a timeout, a
            // cancelled future -- is a defect, and is left to propagate like one.
            if (!execution.isCancelled) throw e
            EXIT_INTERRUPTED
        }

    /**
     * Cancels the running command when the application shuts down underneath it, and waits for
     * it to finish.
     *
     * Ctrl+C shuts the JVM down while a command may still be running -- `fact subscribe` always
     * is. Quarkus fires this from its shutdown hook before it destroys the beans, so the
     * in-flight call ends here, before its channel is closed: the user asked to quit, which is
     * not an error to report, and the channel need not wait for a stream that never ends.
     *
     * Cancelling alone would only ask: the hook would go on to close the channel while the
     * command unwinds, and the stream could still fail as if the server were gone. The wait is
     * bounded, because a blocking read of stdin does not answer to cancellation.
     */
    fun onShutdown(@Observes event: ShutdownEvent) {
        runBlocking { withTimeoutOrNull(CANCELLATION_GRACE) { execution.cancelAndJoin() } }
    }

}

/**
 * Parses and runs [args], turning the outcome into an exit code.
 *
 * Clikt's own `main` would call `exitProcess`, which would cut Quarkus' shutdown short, so the
 * command is parsed directly and the code is returned instead. Anything that is not a
 * [CliktError] or a [FactStoreException] is a defect rather than a diagnosis, and is left to
 * propagate with its stack trace.
 */
private suspend fun SuspendingCliktCommand.execute(args: List<String>): Int =
    try {
        parse(args)
        EXIT_OK
    } catch (e: CliktError) {
        // Covers --help and --version as well as usage errors: Clikt reports all three this way.
        echoFormattedHelp(e)
        e.statusCode
    } catch (e: FactStoreException) {
        echo("❌ ${e.toCliMessage()}", err = true)
        EXIT_FAILURE
    }
