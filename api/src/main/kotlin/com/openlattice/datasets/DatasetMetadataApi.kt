package com.openlattice.datasets

import com.openlattice.authorization.AclKey
import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.PATCH
import retrofit2.http.POST
import retrofit2.http.Path
import java.util.*

interface DataSetMetadataApi {

    companion object {
        const val SERVICE = "/datastore"
        const val CONTROLLER = "/metadata"
        const val BASE = SERVICE + CONTROLLER

        const val COLUMN_PATH = "/column"
        const val DATASET_PATH = "/dataset"
        const val UPDATE_PATH = "/update"

        const val ID = "id"
        const val ID_PATH = "/{$ID}"
        const val DATASET_ID = "datasetId"
        const val DATASET_ID_PATH = "/{$DATASET_ID}"
    }

    /**
     * Gets a dataset using its id
     *
     * @param datasetId The id of the dataset
     *
     * @return The [DataSet] with the specified id
     */
    @GET(BASE + DATASET_PATH + ID_PATH)
    fun getDataset(@Path(ID) datasetId: UUID): DataSet

    /**
     * Gets datasets as a map using their ids
     *
     * @param datasetIds The ids of the datasets to load
     *
     * @return A map from dataset id to [DataSet]
     */
    @POST(BASE + DATASET_PATH)
    fun getDatasets(@Body datasetIds: Set<UUID>): Map<UUID, DataSet>

    /**
     * Gets a dataset column using its id
     *
     * @param datasetId The id of the dataset the column belongs to
     * @param datasetColumnId The id of the column
     *
     * @return The [DatasetColumn] with the aclKey of [datasetId, datasetColumnId]
     */
    @GET(BASE + COLUMN_PATH + DATASET_ID_PATH + ID_PATH)
    fun getDatasetColumn(@Path(DATASET_ID) datasetId: UUID, @Path(ID) datasetColumnId: UUID): DatasetColumn

    /**
     * Gets dataset columns as a map using their aclKeys
     *
     * @param datasetColumnAclKeys The aclKeys of the dataset columns to load
     *
     * @return A map from dataset column [AclKey] to [DatasetColumn]
     */
    @POST(BASE + COLUMN_PATH)
    fun getDatasetColumns(@Body datasetColumnAclKeys: Set<AclKey>): Map<AclKey, DatasetColumn>

    /**
     * Gets all columns in the specified set of dataset ids
     *
     * @param datasetIds The ids of the datasets to load columns in
     *
     * @return A map from dataset id to an iterable of all the [DatasetColumn]s in that dataset
     */
    @POST(BASE + DATASET_PATH + COLUMN_PATH)
    fun getColumnsInDatasets(@Body datasetIds: Set<UUID>): Map<UUID, Iterable<DatasetColumn>>


    /**
     * Updates metadata for the dataset with id [id]
     *
     * @param id The id of the dataset to update metadata for
     *
     */
    @PATCH(BASE + UPDATE_PATH + ID_PATH)
    fun updateDatasetMetadata(@Path(ID) id: UUID, @Body update: SecurableObjectMetadataUpdate)

    /**
     * Updates metadata for the dataset column with aclKey [datasetId, id]
     *
     * @param datasetId The id of the dataset to update metadata for
     * @param id The id of the column in the dataset to update metadata for
     *
     */
    @PATCH(BASE + UPDATE_PATH + DATASET_ID_PATH + ID_PATH)
    fun updateDatasetColumnMetadata(
            @Path(DATASET_ID) datasetId: UUID,
            @Path(ID) id: UUID,
            @Body update: SecurableObjectMetadataUpdate
    )

}
