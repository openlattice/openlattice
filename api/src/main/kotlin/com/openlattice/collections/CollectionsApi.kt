package com.openlattice.collections

import com.openlattice.edm.collection.CollectionTemplateType
import com.openlattice.edm.collection.EntitySetCollection
import com.openlattice.edm.collection.EntityTypeCollection
import com.openlattice.edm.requests.MetadataUpdate
import retrofit2.http.*
import java.util.*

interface CollectionsApi {

    companion object {
        /* These determine the service routing */
        const val SERVICE = "/datastore"
        const val CONTROLLER = "/collections"
        const val BASE = SERVICE + CONTROLLER

        const val ENTITY_TYPE_PATH = "/entity/type"
        const val ENTITY_SET_PATH = "/entity/set"
        const val TEMPLATE_PATH = "/template"

        const val ENTITY_TYPE_ID = "entityTypeId"
        const val ENTITY_TYPE_ID_PATH = "/{$ENTITY_TYPE_ID}"

        const val ENTITY_TYPE_COLLECTION_ID = "entityTypeCollectionId"
        const val ENTITY_TYPE_COLLECTION_ID_PATH = "/{$ENTITY_TYPE_COLLECTION_ID}"

        const val ENTITY_SET_COLLECTION_ID = "entitySetCollectionId"
        const val ENTITY_SET_COLLECTION_ID_PATH = "/{$ENTITY_SET_COLLECTION_ID}"

        const val TYPE_ID = "typeId"
        const val TYPE_ID_PATH = "/{$TYPE_ID}"
    }

    /**
     * Returns all EntityTypeCollection objects
     */
    @GET(BASE + ENTITY_TYPE_PATH)
    fun getAllEntityTypeCollections(): Iterable<EntityTypeCollection>

    /**
     * Returns all authorized EntitySetCollection objects
     */
    @GET(BASE + ENTITY_SET_PATH)
    fun getAllEntitySetCollections(): Iterable<EntitySetCollection>

    /**
     * Returns the EntityTypeCollection object for a given id
     */
    @GET(BASE + ENTITY_TYPE_PATH + ENTITY_TYPE_COLLECTION_ID_PATH)
    fun getEntityTypeCollection(@Path(ENTITY_TYPE_COLLECTION_ID) entityTypeCollectionId: UUID): EntityTypeCollection

    /**
     * Returns the EntitySetCollection object for a given id
     */
    @GET(BASE + ENTITY_SET_PATH + ENTITY_SET_COLLECTION_ID_PATH)
    fun getEntitySetCollection(@Path(ENTITY_SET_COLLECTION_ID) entitySetCollectionId: UUID): EntitySetCollection

    /**
     * Returns all authorized EntitySetCollections for a given EntityTypeCollection id
     */
    @GET(BASE + ENTITY_SET_PATH + ENTITY_TYPE_PATH + ENTITY_TYPE_COLLECTION_ID_PATH)
    fun getEntitySetCollectionsOfType(@Path(ENTITY_TYPE_COLLECTION_ID) entityTypeCollectionId: UUID): Iterable<EntitySetCollection>

    /**
     * Creates a new EntityTypeCollection
     */
    @POST(BASE + ENTITY_TYPE_PATH)
    fun createEntityTypeCollection(@Body entityTypeCollection: EntityTypeCollection): UUID

    /**
     * Creates a new EntitySetCollection and creating any EntitySets missing from the template
     */
    @POST(BASE + ENTITY_SET_PATH)
    fun createEntitySetCollection(@Body entitySetCollection: EntitySetCollection): UUID

    /**
     * Updates metadata of the specified EntityTypeCollection
     */
    @PATCH(BASE + ENTITY_TYPE_PATH + ENTITY_TYPE_COLLECTION_ID_PATH)
    fun updateEntityTypeCollectionMetadata(
            @Path(ENTITY_SET_COLLECTION_ID) entityTypeCollectionId: UUID,
            @Body metadataUpdate: MetadataUpdate)

    /**
     * Appends type to template of the specified EntityTypeCollection
     */
    @PATCH(BASE + ENTITY_TYPE_PATH + ENTITY_TYPE_COLLECTION_ID_PATH + TEMPLATE_PATH)
    fun addTypeToEntityTypeCollectionTemplate(
            @Path(ENTITY_TYPE_COLLECTION_ID) entityTypeCollectionId: UUID,
            @Body collectionTemplateType: CollectionTemplateType)

    /**
     * Removes a key from an EntityTypeCollection template
     */
    @DELETE(BASE + ENTITY_TYPE_PATH + ENTITY_TYPE_COLLECTION_ID_PATH + TEMPLATE_PATH + TYPE_ID_PATH)
    fun removeTypeFromEntityTypeCollectionTemplate(
            @Path(ENTITY_TYPE_COLLECTION_ID) entityTypeCollectionId: UUID,
            @Path(TYPE_ID) typeId: UUID)

    /**
     * Updates metadata of the specified EntitySetCollection
     */
    @PATCH(BASE + ENTITY_SET_PATH + ENTITY_SET_COLLECTION_ID_PATH)
    fun updateEntitySetCollectionMetadata(
            @Path(ENTITY_SET_COLLECTION_ID) entitySetCollectionId: UUID,
            @Body metadataUpdate: MetadataUpdate)

    /**
     * Updates template of the specified EntitySetCollection
     */
    @PATCH(BASE + ENTITY_SET_PATH + ENTITY_SET_COLLECTION_ID_PATH + TEMPLATE_PATH)
    fun updateEntitySetCollectionTemplate(
            @Path(ENTITY_SET_COLLECTION_ID) entitySetCollectionId: UUID,
            @Body templateUpdates: Map<UUID, UUID>)

    /**
     * Deletes the specified EntityTypeCollection
     */
    @DELETE(BASE + ENTITY_TYPE_PATH + ENTITY_TYPE_COLLECTION_ID_PATH)
    fun deleteEntityTypeCollection(@Path(ENTITY_TYPE_COLLECTION_ID) entityTypeCollectionId: UUID)

    /**
     * Deletes the specified EntitySetCollection
     */
    @DELETE(BASE + ENTITY_SET_PATH + ENTITY_SET_COLLECTION_ID_PATH)
    fun deleteEntitySetCollection(@Path(ENTITY_SET_COLLECTION_ID) entitySetCollectionId: UUID)
}