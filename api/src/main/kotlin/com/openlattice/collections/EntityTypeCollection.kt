package com.openlattice.collections


import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.authorization.securable.AbstractSchemaAssociatedSecurableType
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.client.serialization.SerializationConstants
import org.apache.olingo.commons.api.edm.FullQualifiedName

import java.util.*

import com.google.common.base.Preconditions.checkArgument

class EntityTypeCollection

/**
 * Creates an entity type collection with provided parameters and will automatically generate a UUID if not provided.
 *
 * @param id An optional UUID for the entity type collection.
 * @param type The FQN of the entity type collection.
 * @param title The friendly name for the entity type collection.
 * @param description A description of the entity type collection.
 * @param schemas A list of schemas the entity type collection should belong to.
 * @param template A set of CollectionTemplateType objects, which describe the entity types involved in the
 * entity type collection and the purposes they serve
 */
@JsonCreator
constructor(
        @JsonProperty(SerializationConstants.ID_FIELD) id: Optional<UUID>,
        @JsonProperty(SerializationConstants.TYPE_FIELD) type: FullQualifiedName,
        @JsonProperty(SerializationConstants.TITLE_FIELD) title: String,
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) description: Optional<String>,
        @JsonProperty(SerializationConstants.SCHEMAS) schemas: Set<FullQualifiedName>,
        @JsonProperty(SerializationConstants.TEMPLATE) val template: LinkedHashSet<CollectionTemplateType>
) : AbstractSchemaAssociatedSecurableType(id, type, title, description, schemas) {

    init {

        /** Validate template objects  */
        val idsSeen = HashSet<UUID>(template.size)
        val namesSeen = HashSet<String>(template.size)
        template.forEach { (id1, name) ->
            checkArgument(!idsSeen.contains(id1),
                    "EntityTypeCollection template type ids must be distinct.")
            checkArgument(!namesSeen.contains(name),
                    "EntityTypeCollection template type names must be distinct.")

            idsSeen.add(id1)
            namesSeen.add(name)
        }
    }

    constructor(
            id: UUID,
            type: FullQualifiedName,
            title: String,
            description: Optional<String>,
            schemas: Set<FullQualifiedName>,
            template: LinkedHashSet<CollectionTemplateType>
    ) : this(Optional.of<UUID>(id), type, title, description, schemas, template)

    fun addTypeToTemplate(type: CollectionTemplateType) {
        template.add(type)
    }

    fun removeTemplateTypeFromTemplate(id: UUID) {
        template.removeIf { (id1) -> id1 == id }
    }

    fun removeTemplateTypeFromTemplate(name: String) {
        template.removeIf { (_, name1) -> name1 == name }
    }

    @JsonIgnore
    override fun getCategory(): SecurableObjectType {
        return SecurableObjectType.EntityTypeCollection
    }

}
