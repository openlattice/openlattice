package com.openlattice.principals

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AclKeySet

class AddPrincipalToPrincipalEntryProcessor(
        val newAclKey: AclKey
) : AbstractRhizomeEntryProcessor<AclKey, AclKeySet?, AclKey?>() {

    override fun process(entry: MutableMap.MutableEntry<AclKey, AclKeySet?>): AclKey? {
        val currentChildPrincipals: AclKeySet = entry.value ?: AclKeySet()

        return if (currentChildPrincipals.contains(newAclKey)) {
            null
        } else {
            currentChildPrincipals.add(newAclKey)
            entry.setValue(currentChildPrincipals)
            entry.key
        }
    }

    fun getAclKey(): AclKey {
        return newAclKey
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as AddPrincipalToPrincipalEntryProcessor

        if (newAclKey != other.newAclKey) return false

        return true
    }

    override fun hashCode(): Int {
        return newAclKey.hashCode()
    }

}