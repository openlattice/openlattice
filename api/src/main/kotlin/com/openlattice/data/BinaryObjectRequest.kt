package com.openlattice.data

import com.openlattice.authorization.AclKey
import java.util.*

/**
 * This class contains a single field, mapping:
 * entitySetId -> entityKeyId -> propertyTypeId -> digest -> content disposition (or null, to use the default)
 */
class BinaryObjectRequest(val value: Map<UUID, Map<UUID, Map<UUID, Map<String, String?>>>>) {

    fun getAclKeys(): List<AclKey> {
        val entitySetAclKeys = value.keys.map { AclKey(it) }
        val propertyTypeAclKeys = value.flatMap { (entitySetId, map) ->
            map.values.flatMap { it.keys }.toSet().map { AclKey(entitySetId, it) }
        }

        return entitySetAclKeys + propertyTypeAclKeys
    }

    fun mapToS3KeysToDispositions(): Map<String, String?> {
        val map = mutableMapOf<String, String?>()
        value.forEach { (esId, ekToPtToDigestToDispo) ->
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

}