package com.openlattice.organizations

import retrofit2.http.*
import java.util.*


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface DatasourcesApi {
    companion object {
        // @formatter:off
        const val SERVICE = "/datastore"
        const val CONTROLLER = "/organization"
        const val BASE = SERVICE + CONTROLLER
        // @formatter:on

        const val ID = "id"
        const val ID_PATH = "/{$ID}"
        const val DATASOURCE = "/datasource"
        const val DATASOURCE_ID = "datasource_id"
        const val DATASOURCE_ID_PATH = "/{$DATASOURCE_ID}"
    }

    @GET(BASE + ID_PATH + DATASOURCE)
    fun listDatasources(@Path(ID) organizationId: UUID): List<JdbcConnection>

    /**
     * @return The id assigned to the newly registered datasource.
     */
    @POST(BASE + ID_PATH + DATASOURCE)
    fun registerDatasource(
            @Path(ID) organizationId: UUID,
            @Body datasource: JdbcConnection
    ): UUID

    @PUT(BASE + ID_PATH + DATASOURCE + DATASOURCE_ID_PATH)
    fun updateDatasource(
            @Path(ID) organizationId: UUID,
            @Path(DATASOURCE) datasourceId: UUID,
            @Body datasource: JdbcConnection
    )
}