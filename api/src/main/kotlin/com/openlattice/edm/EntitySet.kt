package com.openlattice.edm

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.data.DataExpiration
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.entitysets.StorageType
import org.apache.commons.lang3.StringUtils
import java.util.*


/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 * Describes an entity set and associated metadata, including the active audit record entity set.
 *
 * TODO: Ensure that we are being consistent around how internal sets are accessed and modified. i.e is it okay
 * to return modifiable versions of linked entity sets or should we expose add/remove/update methods. The latter seems
 * like the most explicitly safe thing to do.
 */

data class EntitySet
/**
 * Creates an entity set with provided parameters and will automatically generate a UUID if not provided.
 *
 * @param _id An optional UUID for the entity set.
 * @param name The name of the entity set.
 * @param _title The friendly name for the entity set.
 * @param _description A description of the entity set.
 */
@JvmOverloads constructor(
        @JsonProperty(SerializationConstants.ID_FIELD) private val _id: UUID = UUID.randomUUID(),
        @JsonProperty(SerializationConstants.ENTITY_TYPE_ID) val entityTypeId: UUID,
        @JsonProperty(SerializationConstants.NAME_FIELD) var name: String,
        @JsonProperty(SerializationConstants.TITLE_FIELD) val _title: String,
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) val _description: String = "",
        @JsonProperty(SerializationConstants.CONTACTS) var contacts: MutableSet<String>,
        @JsonProperty(
                SerializationConstants.LINKED_ENTITY_SETS
        ) val linkedEntitySets: MutableSet<UUID> = mutableSetOf(),
        @JsonProperty(SerializationConstants.ORGANIZATION_ID) var organizationId: UUID,
        @JsonProperty(SerializationConstants.FLAGS_FIELD) val flags: EnumSet<EntitySetFlag> =
                EnumSet.of(EntitySetFlag.EXTERNAL),
        @JsonProperty(SerializationConstants.PARTITIONS) val partitions: LinkedHashSet<Int> = linkedSetOf(),
        @JsonProperty(SerializationConstants.EXPIRATION) var expiration: DataExpiration? = null,
        @JsonProperty(SerializationConstants.STORAGE_TYPE) val storageType: StorageType = StorageType.OBJECT
) : AbstractSecurableObject(_id, _title, _description) {

    init {
        require(StringUtils.isNotBlank(name)) { "Entity set name cannot be blank." }
        require(this.linkedEntitySets.isEmpty() || isLinking) {
            "You cannot specify linked entity sets unless this is a linking entity set."
        }
    }

    val isExternal: Boolean
        @JsonIgnore
        get() = flags.contains(EntitySetFlag.EXTERNAL)

    val isLinking: Boolean
        @JsonIgnore
        get() = flags.contains(EntitySetFlag.LINKING)

    var partitionsVersion: Int = 0

    @JsonIgnore
    fun setPartitions(partitions: Collection<Int>) {
        this.partitions.clear()
        addPartitions(partitions)
    }

    fun addFlag(flag: EntitySetFlag) {
        this.flags.add(flag)
    }

    fun removeFlag(flag: EntitySetFlag) {
        this.flags.remove(flag)
    }

    internal fun addPartitions(partitions: Collection<Int>) {
        this.partitions.addAll(partitions)
    }

    @JsonIgnore
    fun hasExpirationPolicy(): Boolean {
        return expiration != null
    }

    @JsonIgnore
    override fun getCategory() = SecurableObjectType.EntitySet
}
