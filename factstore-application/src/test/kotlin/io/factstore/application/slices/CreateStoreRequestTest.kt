package io.factstore.application.slices

import io.factstore.application.fingerprint
import io.factstore.core.Fingerprint
import io.factstore.core.StoreId
import io.factstore.core.StoreName
import io.factstore.domain.CreateStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CreateStoreRequestTest {

    private val orders = StoreName("orders")

    @Test
    fun `becomes a command for a new store under the requested name`() {
        val s1 = StoreId.generate()

        val command = CreateStoreRequest(orders).toCommand { s1 }

        assertThat(command).isEqualTo(CreateStore(s1, orders))
    }

    @Test
    fun `gives requests for other names other fingerprints`() {
        val fingerprint = CreateStoreRequestEncoding.fingerprint(CreateStoreRequest(orders))

        assertThat(fingerprint).isNotEqualTo(CreateStoreRequestEncoding.fingerprint(CreateStoreRequest(StoreName("invoices"))))
    }

    /** Fingerprints are stored with their keys: a change here makes retries across it look like reused keys. */
    @Test
    fun `keeps its encoding`() {
        val fingerprint = CreateStoreRequestEncoding.fingerprint(CreateStoreRequest(orders))

        assertThat(fingerprint).isEqualTo(Fingerprint("5d2b63eba83721929c4aed4c3220bfecc823d2b80007ef67741b2de1192a8704"))
    }
}
