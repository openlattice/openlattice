package com.openlattice.shuttle.payload

import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.openlattice.shuttle.source.IntegrationOrigin
import com.openlattice.shuttle.source.LocalFileOrigin
import org.slf4j.LoggerFactory
import java.nio.file.Paths

data class XmlFilesPayload(val origin: IntegrationOrigin) : Payload {

    constructor(source: String) : this(LocalFileOrigin(Paths.get(source)) { it.toString().endsWith(XML_SUFFIX) })

    companion object {
        private val logger = LoggerFactory.getLogger(XmlFilesPayload::class.java)
        private const val XML_SUFFIX = ".xml"
        private val mapper = XmlMapper()
    }

    override fun getPayload(): Iterable<MutableMap<String, Any?>> {
        return origin.map {
            val value = mapper.readValue<MutableMap<String, Any?>>(it)
            return@map recFlatten(value).toMap(mutableMapOf())
        }.asIterable()
    }

    private fun recFlatten(map: Map<String, Any?>, keyPrefix: String = ""): List<Pair<String, Any?>> {
        val prefix = if (!keyPrefix.isBlank()) "$keyPrefix." else keyPrefix

        return map.flatMap { (key, value) ->
            if (value is Map<*, *>) {
                return@flatMap recFlatten(value as Map<String, Any?>, "$prefix$key")
            }
            return@flatMap listOf("$prefix$key" to value)
        }
    }
}