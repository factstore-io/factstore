package io.factstore.domain

import com.fraktalio.fmodel.domain.Decider
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking

class DeciderScenario<C, S, E>(private val decider: Decider<C, S, E>, private val given: List<E>) {

    infix fun whenever(command: C): List<E> = runBlocking {
        val state = given.fold(decider.initialState, decider.evolve)
        decider.decide(command, state).toList()
    }
}

fun <C, S, E> Decider<C, S, E>.given(vararg facts: E) = DeciderScenario(this, facts.toList())
