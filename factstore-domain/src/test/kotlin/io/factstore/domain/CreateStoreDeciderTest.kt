package io.factstore.domain

import io.factstore.core.StoreId
import io.factstore.core.StoreName
import io.factstore.domain.StoreCreationRejected.Reason.NameTaken
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CreateStoreDeciderTest {

    private val s1 = StoreId.generate()
    private val s2 = StoreId.generate()
    private val s3 = StoreId.generate()
    private val orders = StoreName("orders")

    @Test
    fun `creates the store when the name has never been used`() {
        val facts = createStoreDecider.given() whenever CreateStore(s1, orders)

        assertThat(facts).containsExactly(StoreCreated(s1, orders))
    }

    @Test
    fun `rejects the store while another store holds the name, naming the holder`() {
        val facts = createStoreDecider.given(StoreCreated(s1, orders)) whenever CreateStore(s2, orders)

        assertThat(facts).containsExactly(StoreCreationRejected(s2, orders, NameTaken(holder = s1)))
    }

    @Test
    fun `accepts a name another store holds only under a different name`() {
        val facts = createStoreDecider.given(StoreCreated(s1, StoreName("sales"))) whenever CreateStore(s2, orders)

        assertThat(facts).containsExactly(StoreCreated(s2, orders))
    }

    @Test
    fun `is not influenced by earlier rejections`() {
        val facts = createStoreDecider.given(StoreCreationRejected(s2, orders, NameTaken(holder = s1))) whenever CreateStore(s3, orders)

        assertThat(facts).containsExactly(StoreCreated(s3, orders))
    }
}
