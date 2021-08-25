package com.openlattice.collaborations

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

data class Collaboration
@JvmOverloads constructor(
        @JsonProperty(SerializationConstants.ID_FIELD) val _id: UUID = UUID.randomUUID(),
        val name: String,
        @JsonProperty(SerializationConstants.TITLE_FIELD) val _title: String,
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) val _description: String = "",
        var organizationIds: MutableSet<UUID> = mutableSetOf()
) : AbstractSecurableObject(_id, _title, _description) {

    @JsonIgnore
    override fun getCategory(): SecurableObjectType {
        return SecurableObjectType.Collaboration
    }

    fun addOrganizationIds(ids: Iterable<UUID>) {
        organizationIds.addAll(ids)
    }

    fun removeOrganizationIds(ids: Iterable<UUID>) {
        organizationIds.removeAll(ids)
    }
}