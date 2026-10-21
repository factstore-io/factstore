package io.factstore.cli.output

/** The shapes facts can be written in. */
enum class OutputFormat(val cliName: String) {

    /** A table, sized to its contents. */
    Table("table"),

    /** One pretty-printed JSON array. */
    Json("json"),

    /** One compact JSON object per line, as the HTTP API streams them. */
    Ndjson("ndjson"),
}
