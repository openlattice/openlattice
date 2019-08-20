package com.openlattice.collections


import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Preconditions.checkArgument
import com.openlattice.authorization.securable.AbstractSchemaAssociatedSecurableType
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.client.serialization.SerializationConstants
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*

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
        template.forEach {
            checkArgument(!idsSeen.contains(it.id),
                    "EntityTypeCollection template type ids must be distinct.")
            checkArgument(!namesSeen.contains(it.name),
                    "EntityTypeCollection template type names must be distinct.")

            idsSeen.add(it.id)
            namesSeen.add(it.name)
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
        template.removeIf { it.id == id }
    }

    fun removeTemplateTypeFromTemplate(name: String) {
        template.removeIf { it.name == name }
    }

    @JsonIgnore
    override fun getCategory(): SecurableObjectType {
        return SecurableObjectType.EntityTypeCollection
    }

}
