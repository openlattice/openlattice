package com.openlattice.principals

import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AclKeySet
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor

class AddPrincipalToPrincipalEntryProcessor(
        val newAclKey: AclKey
) : AbstractReadOnlyRhizomeEntryProcessor<AclKey, AclKeySet?, AclKey?>() {

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
}