package com.cassisi.openeventstore.core

import kotlinx.serialization.json.*

// Immutable representation of a single index entry (tuple-style)
data class IndexEntry(
    val pathSegments: List<String>
)

// Immutable index of a JSON object
data class TupleGinIndex(
    val entries: List<IndexEntry>
)

// Function to flatten a JSON object into tuple-style keys
fun JsonObject.buildTupleGinIndex(): TupleGinIndex {
    val entries = mutableListOf<IndexEntry>()

    fun traverse(path: List<String>, element: JsonElement) {
        when (element) {
            is JsonObject -> element.forEach { (k, v) ->
                traverse(path + k, v)
            }
            is JsonArray -> element.forEach { v ->
                traverse(path, v) // arrays reuse the same path
            }
            is JsonPrimitive -> entries.add(IndexEntry(path + element.content))
        }
    }

    traverse(emptyList(), this)
    return TupleGinIndex(entries)
}


fun main() {
    val json = Json.parseToJsonElement(
        """
        {
          "firstName": "John",
          "lastName": "Smith",
          "age": 25,
          "address": {
            "state": "NY",
            "postalCode": "10021"
          },
          "cars": ["Subaru", "Honda"],
          "addresses": [
            {
              "street": "mystreet1"
            },
            {
              "street": "mystreet21"
            }
          ]
        }
        """
    ).jsonObject

    val index = json.buildTupleGinIndex()

    println("Immutable index dump:")
    index.entries.forEach { println(it.pathSegments) }
}