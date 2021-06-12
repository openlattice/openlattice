package com.openlattice.datasets

import com.openlattice.authorization.Permission
import com.openlattice.organizations.Organization
import retrofit2.http.*
import java.util.*

interface DataSetMetadataApi {

    companion object {

        const val SERVICE = "/datastore"
        const val CONTROLLER = "/metadata"
        const val BASE = SERVICE + CONTROLLER

        const val COLUMNS_PATH = "/columns"
        const val DATA_SETS_PATH = "/datasets"
        const val ORGANIZATIONS_PATH = "/organizations"
        const val UPDATE_PATH = "/update"

        const val COLUMN_ID_PARAM = "columnId"
        const val COLUMN_ID_PATH = "/{$COLUMN_ID_PARAM}"
        const val DATA_SET_ID_PARAM = "dataSetId"
        const val DATA_SET_ID_PATH = "/{$DATA_SET_ID_PARAM}"
        const val ORGANIZATION_ID_PARAM = "organizationId"
        const val ORGANIZATION_ID_PATH = "/{$ORGANIZATION_ID_PARAM}"
    }

    /**
     * Gets the [DataSet] metadata object with the given data set id. The caller must have [Permission.READ] on the
     * target [DataSet] metadata object.
     *
     * @param dataSetId a data set id
     * @return the target [DataSet] metadata object
     */
    @GET(BASE + DATA_SETS_PATH + DATA_SET_ID_PATH)
    fun getDataSetMetadata(@Path(DATA_SET_ID_PARAM) dataSetId: UUID): DataSet

    /**
     * Gets the [DataSet] metadata objects with the given data set ids that the caller has [Permission.READ] on.
     *
     * @param dataSetIds a set of data set ids
     * @return Map<K, V> where K is a data set id and V is a [DataSet] object
     */
    @POST(BASE + DATA_SETS_PATH)
    fun getDataSetsMetadata(@Body dataSetIds: Set<UUID>): Map<UUID, DataSet>

    /**
     * Gets the [DataSetColumn] metadata object with the given data set id and column id. The caller must have
     * [Permission.READ] on the target [DataSetColumn] metadata object.
     *
     * @param dataSetId a data set id
     * @param columnId a data set column id
     * @return the target [DataSetColumn] metadata object
     */
    @GET(BASE + COLUMNS_PATH + DATA_SET_ID_PATH + COLUMN_ID_PATH)
    fun getDataSetColumnMetadata(
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
    fun getDataSetColumnsMetadata(@Body dataSetIds: Set<UUID>): Map<UUID, List<DataSetColumn>>

    /**
     * Gets all [DataSet] metadata objects the caller has [Permission.READ] on that belong to the [Organization] object
     * with the given organization id. The caller must have [Permission.READ] on the target [Organization] object.
     *
     * @param organizationId [Organization] id
     * @return Map<K, V> where K is a data set id and V is a [DataSet] object
     */
    @GET(BASE + DATA_SETS_PATH + ORGANIZATIONS_PATH + ORGANIZATION_ID_PATH)
    fun getOrganizationDataSetsMetadata(@Path(ORGANIZATION_ID_PARAM) organizationId: UUID): Map<UUID, DataSet>

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
     * Applies the given metadata updates to the data set column with the given data set id and column id. The caller
     * must have [Permission.OWNER] on the target data set column.
     *
     * @param dataSetId a data set id
     * @param columnId a data set column id
     * @param metadata the metadata updates to apply
     */
    @PATCH(BASE + UPDATE_PATH + DATA_SET_ID_PATH + COLUMN_ID_PATH)
    fun updateDataSetColumnMetadata(
        @Path(DATA_SET_ID_PARAM) dataSetId: UUID,
        @Path(COLUMN_ID_PARAM) columnId: UUID,
        @Body metadata: SecurableObjectMetadataUpdate
    )
}
