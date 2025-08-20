package com.cassisi.openeventstore

import com.apple.foundationdb.Database
import com.apple.foundationdb.tuple.Tuple
import io.quarkus.runtime.Startup
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes

@Startup
@ApplicationScoped
class MyDb(
    private val database: Database
) {

    fun onStart(@Observes startupEvent: StartupEvent) {
        println("on startup...")
        println(database)

        println("executing first run...")
        database.run { tr ->
            println("inside run")
             tr.set(Tuple.from("hello").pack(), Tuple.from("world").pack());
            return@run
        }

        println("executing second run...")
        val value: String = database.run { tr ->
            val result = tr[Tuple.from("hello").pack()].join()
            Tuple.fromBytes(result).getString(0)
        }

        println("Hello $value")

      //  database.close()
    }

}