package io.factstore.server.grpc

import io.factstore.grpc.v1.*
import io.quarkus.grpc.GrpcClient
import io.quarkus.test.junit.QuarkusTest
import io.smallrye.mutiny.coroutines.awaitSuspending
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.jupiter.api.*
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import java.util.UUID

@QuarkusTest
@TestMethodOrder(OrderAnnotation::class)
@TestInstance(PER_CLASS)
class GrpcStoreServiceTest {

    @GrpcClient
    lateinit var storeService: StoreService

    @BeforeAll
    fun setUp(): Unit = runBlocking {
        storeService.createStore(createStoreRequest { name = "grpc-store-a" }).awaitSuspending()
    }

    @Test
    @Order(1)
    @DisplayName("CreateStore - should return StoreCreated with a valid UUID when name is available")
    fun createStore(): Unit = runBlocking {
        val response = storeService.createStore(createStoreRequest { name = "grpc-store-new" }).awaitSuspending()

        assertThat(response.hasCreated()).isTrue()
        assertThat(response.created.id).isNotBlank()
        assertThatCode { UUID.fromString(response.created.id) }.doesNotThrowAnyException()
    }

    @Test
    @Order(2)
    @DisplayName("CreateStore - should return NameAlreadyExists when name is already taken")
    fun createStoreDuplicate(): Unit = runBlocking {
        val response = storeService.createStore(createStoreRequest { name = "grpc-store-a" }).awaitSuspending()

        assertThat(response.hasNameAlreadyExists()).isTrue()
    }

    @Test
    @Order(3)
    @DisplayName("GetStore - should return StoreFound with store info when store exists")
    fun getStore(): Unit = runBlocking {
        val response = storeService.getStore(getStoreRequest { name = "grpc-store-a" }).awaitSuspending()

        assertThat(response.hasFound()).isTrue()
        assertThat(response.found.store.name).isEqualTo("grpc-store-a")
        assertThat(response.found.store.id).isNotBlank()
        assertThat(response.found.store.hasCreatedAt()).isTrue()
    }

    @Test
    @Order(4)
    @DisplayName("GetStore - should return StoreNotFound when store does not exist")
    fun getStoreNotFound(): Unit = runBlocking {
        val response = storeService.getStore(getStoreRequest { name = "no-such-store" }).awaitSuspending()

        assertThat(response.hasNotFound()).isTrue()
        assertThat(response.notFound.storeName).isEqualTo("no-such-store")
    }

    @Test
    @Order(5)
    @DisplayName("StoreExists - should return exists=true when store is present")
    fun storeExists(): Unit = runBlocking {
        val response = storeService.storeExists(storeExistsRequest { name = "grpc-store-a" }).awaitSuspending()

        assertThat(response.hasPresent()).isTrue()
    }

    @Test
    @Order(6)
    @DisplayName("StoreExists - should return exists=false when store is absent")
    fun storeDoesNotExist(): Unit = runBlocking {
        val response = storeService.storeExists(storeExistsRequest { name = "no-such-store" }).awaitSuspending()

        assertThat(response.hasAbsent()).isTrue()
    }

    @Test
    @Order(7)
    @DisplayName("ListStores - should include all created stores")
    fun listStores(): Unit = runBlocking {
        storeService.createStore(createStoreRequest { name = "grpc-store-b" }).awaitSuspending()

        val response = storeService.listStores(listStoresRequest { }).awaitSuspending()

        val names = response.storesList.map { it.name }
        assertThat(names).contains("grpc-store-a", "grpc-store-b")
    }

    @Test
    @Order(8)
    @DisplayName("DeleteStore - should return StoreDeleted when store exists")
    fun deleteStore(): Unit = runBlocking {
        val response = storeService.deleteStore(deleteStoreRequest { name = "grpc-store-a" }).awaitSuspending()

        assertThat(response.hasDeleted()).isTrue()
    }

    @Test
    @Order(9)
    @DisplayName("DeleteStore - should return StoreNotFound when store does not exist")
    fun deleteStoreNotFound(): Unit = runBlocking {
        val response = storeService.deleteStore(deleteStoreRequest { name = "grpc-store-a" }).awaitSuspending()

        assertThat(response.hasNotFound()).isTrue()
        assertThat(response.notFound.storeName).isEqualTo("grpc-store-a")
    }
}
