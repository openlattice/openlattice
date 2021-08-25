package com.openlattice.edm.requests

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Preconditions
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.data.DataExpiration
import com.openlattice.postgres.IndexType
import org.apache.commons.lang3.StringUtils
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*


/**
 * Used for updating metadata of property type, entity type, or entity set. Non-existent fields for the specific object
 * would be ignored.
 */
data class MetadataUpdate(
        // Common across property type, entity type, entity set
        @JsonProperty(SerializationConstants.TITLE_FIELD) val title: Optional<String>,
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) val description: Optional<String>,
        // Specific to entity set
        @JsonProperty(SerializationConstants.NAME_FIELD) val name: Optional<String>,
        @JsonProperty(SerializationConstants.CONTACTS) val contacts: Optional<MutableSet<String>>,
        // Specific to property type/entity type
        @JsonProperty(SerializationConstants.TYPE_FIELD) val type: Optional<FullQualifiedName>,
        // Specific to property type
        @JsonProperty(SerializationConstants.PII_FIELD) val pii: Optional<Boolean>,
        // Specific to entity set property type metadata
        @JsonProperty(SerializationConstants.DEFAULT_SHOW) val defaultShow: Optional<Boolean>,
        @JsonProperty(SerializationConstants.URL) val url: Optional<String>,
        @JsonProperty(SerializationConstants.PROPERTY_TAGS)
        val propertyTags: Optional<LinkedHashMap<UUID, LinkedHashSet<String>>>,
        @JsonProperty(SerializationConstants.INDEX_TYPE) val indexType: Optional<IndexType>,
        @JsonProperty(SerializationConstants.ORGANIZATION_ID)
        val organizationId: Optional<UUID>,
        @JsonProperty(SerializationConstants.PARTITIONS)
        val partitions: Optional<LinkedHashSet<Int>>,
        @JsonProperty(SerializationConstants.EXPIRATION) val dataExpiration: Optional<DataExpiration>,
        @JsonProperty(SerializationConstants.DATASTORE) val dataSourceName: Optional<String>
) {


    //    @JsonCreator
//    fun MetadataUpdate(
//            @JsonProperty(SerializationConstants.TITLE_FIELD) title: Optional<String?>,
//            @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) description: Optional<String>?,
//            @JsonProperty(SerializationConstants.NAME_FIELD) name: Optional<String?>,
//            @JsonProperty(SerializationConstants.CONTACTS) contacts: Optional<Set<String?>>,
//            @JsonProperty(SerializationConstants.TYPE_FIELD) type: Optional<FullQualifiedName>,
//            @JsonProperty(SerializationConstants.PII_FIELD) pii: Optional<Boolean>?,
//            @JsonProperty(SerializationConstants.DEFAULT_SHOW) defaultShow: Optional<Boolean>?,
//            @JsonProperty(SerializationConstants.URL) url: Optional<String>?,
//            @JsonProperty(
//                    SerializationConstants.PROPERTY_TAGS
//            ) propertyTags: Optional<LinkedHashMap<UUID?, LinkedHashSet<String?>>>,
//            @JsonProperty(SerializationConstants.INDEX_TYPE) indexType: Optional<IndexType>?,
//            @JsonProperty(SerializationConstants.ORGANIZATION_ID) organizationId: Optional<UUID>?,
//            @JsonProperty(SerializationConstants.PARTITIONS) partitions: Optional<LinkedHashSet<Int>>?,
//            @JsonProperty(SerializationConstants.EXPIRATION) dataExpiration: Optional<DataExpiration>?
//    )
    init {
        // WARNING These checks have to be consistent with the same check elsewhere.
        Preconditions.checkArgument(
                title.isEmpty || StringUtils.isNotBlank(title.get()),
                "Title cannot be blank."
        )
        Preconditions.checkArgument(
                name.isEmpty || StringUtils.isNotBlank(name.get()),
                "Entity set name cannot be blank."
        )
        Preconditions.checkArgument(contacts.isEmpty || !contacts.get().isEmpty(), "Contacts cannot be blank.")
        Preconditions.checkArgument(
                type.isEmpty || StringUtils.isNotBlank(type.get().namespace),
                "Namespace of type is missing."
        )
        Preconditions.checkArgument(
                type.isEmpty || StringUtils.isNotBlank(type.get().name),
                "Name of type is missing."
        )

        propertyTags.ifPresent { tags: LinkedHashMap<UUID, LinkedHashSet<String>> ->
            tags.values.forEach { tagValues: LinkedHashSet<String> ->
                Preconditions.checkArgument(
                        tagValues.isNotEmpty(), "Property tag values cannot be empty."
                )
            }
        }
    }

    @JvmOverloads
    constructor(
            title: Optional<String>,
            description: Optional<String>,
            name: Optional<String>,
            contacts: Optional<MutableSet<String>>,
            type: Optional<FullQualifiedName>,
            pii: Optional<Boolean>,
            defaultShow: Optional<Boolean>,
            url: Optional<String>,
            propertyTags: Optional<LinkedHashMap<UUID, LinkedHashSet<String>>>,
            organizationId: Optional<UUID>,
            partitions: Optional<LinkedHashSet<Int>>,
            dataExpiration: Optional<DataExpiration>,
            dataSourceName: Optional<String> = Optional.empty()
    ) :
            this(
                    title,
                    description,
                    name,
                    contacts,
                    type,
                    pii,
                    defaultShow,
                    url,
                    propertyTags,
                    Optional.empty(),
                    organizationId,
                    partitions,
                    dataExpiration,
                    dataSourceName
            )


    constructor(title: String?, description: String?, contacts: MutableSet<String>?) :
            this(
                    Optional.ofNullable(title),
                    Optional.ofNullable(description),
                    Optional.empty<String>(),
                    Optional.ofNullable(contacts),
                    Optional.empty(),
                    Optional.empty<Boolean>(),
                    Optional.empty<Boolean>(),
                    Optional.empty<String>(),
                    Optional.empty<LinkedHashMap<UUID, LinkedHashSet<String>>>(),
                    Optional.empty(),
                    Optional.empty<UUID>(),
                    Optional.empty<LinkedHashSet<Int>>(),
                    Optional.empty(),
                    Optional.empty()
            )

}