package com.openlattice.data

import com.openlattice.authorization.AclKey
import java.net.URL
import java.util.*

/**
 * This class contains a single field, mapping:
 * entitySetId -> entityKeyId -> propertyTypeId -> digest -> content disposition (or null, to use the default)
 */
class BinaryObjectRequest(
        val esToEkToPtToDigestToDispo: Map<UUID, Map<UUID, Map<UUID, Map<String, String?>>>>
) : Map<UUID, Map<UUID, Map<UUID, Map<String, String?>>>> by esToEkToPtToDigestToDispo {

    fun getAclKeys(): List<AclKey> {
        val entitySetAclKeys = esToEkToPtToDigestToDispo.keys.map { AclKey(it) }
        val propertyTypeAclKeys = esToEkToPtToDigestToDispo.flatMap { (entitySetId, map) ->
            map.values.flatMap { it.keys }.toSet().map { AclKey(entitySetId, it) }
        }

        return entitySetAclKeys + propertyTypeAclKeys
    }

    fun mapToS3KeysToDispositions(): Map<String, String?> {
        val map = mutableMapOf<String, String?>()
        esToEkToPtToDigestToDispo.forEach { (esId, ekToPtToDigestToDispo) ->
            ekToPtToDigestToDispo.forEach { (ekId, ptToDigestToDispo) ->
                ptToDigestToDispo.forEach { (ptId, digestToDispo) ->
                    digestToDispo.forEach { (digest, dispo) ->
                        map["$esId/$ekId/$ptId/$digest"] = dispo
                    }
                }
            }
        }
        return map
    }

    companion object {
        @JvmStatic
        fun mapFromS3KeysToUrls(keyToUrl: Map<String, URL>): Map<UUID, Map<UUID, Map<UUID, Map<String, URL>>>> {
            val result: MutableMap<UUID, MutableMap<UUID, MutableMap<UUID, MutableMap<String, URL>>>> = mutableMapOf()
            keyToUrl.forEach { (key, url) ->
                val (esIdStr, ekIdStr, ptIdStr, digest) = key.split("/")
                val esId = UUID.fromString(esIdStr)
                val ekId = UUID.fromString(ekIdStr)
                val ptId = UUID.fromString(ptIdStr)

                if (!result.containsKey(esId)) {
                    result[esId] = mutableMapOf()
                }
                if (!result.getValue(esId).containsKey(ekId)) {
                    result.getValue(esId)[ekId] = mutableMapOf()
                }
                if (!result.getValue(esId).getValue(ekId).containsKey(ptId)) {
                    result.getValue(esId).getValue(ekId)[ptId] = mutableMapOf()
                }
                result.getValue(esId).getValue(ekId).getValue(ptId)[digest] = url
            }

            return result
        }
    }

}