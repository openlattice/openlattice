package com.openlattice.datasets

import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Permission
import com.openlattice.collaborations.Collaboration
import retrofit2.http.*
import java.util.*

interface DataSetMetadataApi {

    companion object {

        const val SERVICE = "/datastore"
        const val CONTROLLER = "/metadata"
        const val BASE = SERVICE + CONTROLLER

        const val COLUMNS_PATH = "/columns"
        const val DATA_SETS_PATH = "/datasets"
        const val UPDATE_PATH = "/update"

        const val COLUMN_ID_PARAM = "columnId"
        const val COLUMN_ID_PATH = "/{$COLUMN_ID_PARAM}"
        const val DATA_SET_ID_PARAM = "dataSetId"
        const val DATA_SET_ID_PATH = "/{$DATA_SET_ID_PARAM}"
    }

    /**
     * Gets the [DataSet] metadata object with the given data set id. The caller must have [Permission.READ] on the
     * target [DataSet] metadata object.
     *
     * @param dataSetId a data set id
     * @return the target [DataSet] metadata object
     */
    @GET(BASE + DATA_SETS_PATH + DATA_SET_ID_PATH)
    fun getDataSet(@Path(DATA_SET_ID_PARAM) dataSetId: UUID): DataSet

    /**
     * Gets the [DataSet] metadata objects with the given data set ids that the caller has [Permission.READ] on.
     *
     * @param dataSetIds a set of data set ids
     * @return Map<K, V> where K is a data set id and V is a [DataSet] object
     */
    @POST(BASE + DATA_SETS_PATH)
    fun getDataSets(@Body dataSetIds: Set<UUID>): Map<UUID, DataSet>

    /**
     * Gets the [DataSetColumn] metadata object with the given data set id and column id. The caller must have
     * [Permission.READ] on the target [DataSetColumn] metadata object.
     *
     * @param dataSetId a data set id
     * @param columnId a data set column id
     * @return the target [DataSetColumn] metadata object
     */
    @GET(BASE + COLUMNS_PATH + DATA_SET_ID_PATH + COLUMN_ID_PATH)
    fun getDataSetColumn(
        @Path(DATA_SET_ID_PARAM) dataSetId: UUID,
        @Path(COLUMN_ID_PARAM) columnId: UUID
    ): DataSetColumn

    /**
     * Gets all [DataSetColumn] metadata objects that the caller has [Permission.READ] on that belong to data sets
     * with the given data set ids.
     *
     * @param dataSetIds a set of data set ids
     * @return Map<K, V> where K is a data set id and V is a list of [DataSetColumn] metadata objects
     */
    @POST(BASE + COLUMNS_PATH)
    fun getDataSetColumns(@Body dataSetIds: Set<UUID>): Map<UUID, List<DataSetColumn>>

    /**
     * Applies the given metadata updates to the data set with the given data set id. The caller must have
     * [Permission.OWNER] on the target data set.
     *
     * @param dataSetId a data set id
     * @param metadata the metadata updates to apply
     */
    @PATCH(BASE + UPDATE_PATH + DATA_SET_ID_PATH)
    fun updateDataSetMetadata(
        @Path(DATA_SET_ID_PARAM) dataSetId: UUID,
        @Body metadata: SecurableObjectMetadataUpdate
    )

    /**
     * Updates metadata for the dataset column with aclKey [datasetId, id]
     *
     * @param datasetId The id of the dataset to update metadata for
     * @param id The id of the column in the dataset to update metadata for
     *
     */
    @PATCH(BASE + UPDATE_PATH + DATA_SET_ID_PATH + COLUMN_ID_PATH)
    fun updateDataSetColumnMetadata(
        @Path(DATA_SET_ID_PARAM) dataSetId: UUID,
        @Path(COLUMN_ID_PARAM) columnId: UUID,
        @Body update: SecurableObjectMetadataUpdate
    )
}