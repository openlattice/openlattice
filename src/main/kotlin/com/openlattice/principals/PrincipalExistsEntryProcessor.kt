package com.openlattice.principals

import com.openlattice.authorization.AclKey
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor

class PrincipalExistsEntryProcessor :  AbstractReadOnlyRhizomeEntryProcessor<AclKey, SecurablePrincipal?, Boolean>() {

    override fun process(entry: MutableMap.MutableEntry<AclKey, SecurablePrincipal?>): Boolean {
        return entry.value != null
    }
}