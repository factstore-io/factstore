package io.factstore.client.internal

import io.grpc.stub.AbstractStub
import java.util.concurrent.TimeUnit
import kotlin.time.Duration

/** This stub, with a deadline of [timeout] from now for the call made on it. */
internal fun <S : AbstractStub<S>> S.withCallTimeout(timeout: Duration): S =
    withDeadlineAfter(timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
