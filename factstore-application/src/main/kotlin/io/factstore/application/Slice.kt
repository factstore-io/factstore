package io.factstore.application

import com.fraktalio.fmodel.domain.IDecider

/** A decider, its consistency boundary and the repository it reads from and saves to, composed into one. */
interface Slice<C, S, F, Q, A, V> :
    IDecider<C, S, F>,
    ConsistencyBoundary<C, Q, A, V>,
    FactRepository<F, Q, A, V>

fun <C, S, F, Q, A, V> Slice(
    decider: IDecider<C, S, F>,
    boundary: ConsistencyBoundary<C, Q, A, V>,
    repository: FactRepository<F, Q, A, V>,
): Slice<C, S, F, Q, A, V> =
    object : Slice<C, S, F, Q, A, V>,
        IDecider<C, S, F> by decider,
        ConsistencyBoundary<C, Q, A, V> by boundary,
        FactRepository<F, Q, A, V> by repository {}
