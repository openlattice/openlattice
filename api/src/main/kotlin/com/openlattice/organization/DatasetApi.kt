package com.openlattice.organization

import com.openlattice.authorization.Permission
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.postgres.PostgresConnectionType
import retrofit2.http.Body
import retrofit2.http.DELETE
import retrofit2.http.GET
import retrofit2.http.PATCH
import retrofit2.http.POST
import retrofit2.http.Path
import java.util.*


const val SERVICE = "/datastore"
const val CONTROLLER = "/organization-database"
const val BASE = SERVICE + CONTROLLER

const val EXTERNAL_DATABASE = "/external-database"
const val EXTERNAL_DATABASE_COLUMN = "/external-database-column"
const val EXTERNAL_DATABASE_TABLE = "/external-database-table"
const val DATA = "/data"
const val AUTHORIZED = "/authorized"

const val ID = "id"
const val ID_PATH = "/{$ID}"
const val TABLE_NAME = "tableName"
const val TABLE_NAME_PATH = "/{$TABLE_NAME}"
const val TABLE_ID = "tableID"
const val TABLE_ID_PATH = "/{$TABLE_ID}"
const val COLUMN_NAME = "columnName"
const val COLUMN_NAME_PATH = "/{$COLUMN_NAME}"
const val ROW_COUNT = "rowCount"
const val ROW_COUNT_PATH = "/{$ROW_COUNT}"
const val USER_ID = "userId"
const val USER_ID_PATH = "/{$USER_ID}"
const val CONNECTION_TYPE = "connectionType"
const val CONNECTION_TYPE_PATH = "/{$CONNECTION_TYPE}"
const val PERMISSION = "permission"
const val PERMISSION_PATH = "/{$PERMISSION}"
const val SCHEMA_PATH = "/schema"

interface DatasetApi {

    /**
     * Adds an authentication record to the pg_hba config file
     * that restricts database access by ip address for a user
     * @param organizationId The organization's UUID
     * @param userId The user to be given trusted status
     * @param connectionType The connection type via which the user will be trusted
     * @param ipAddress The ip addresses (as string) from which users
     * will be trusted. This should be an empty string if the connectionType is local.
     */
    @POST(BASE + ID_PATH + USER_ID_PATH + CONNECTION_TYPE_PATH + EXTERNAL_DATABASE)
    fun addHBARecord(
            @Path(ID) organizationId: UUID,
            @Path(USER_ID) userId: String,
            @Path(CONNECTION_TYPE) connectionType: PostgresConnectionType,
            @Body ipAddress: String)

    /**
     * Removes an authentication record to the pg_hba config file
     * that restricts database access by ip address for a user
     * @param organizationId The organization's UUID
     * @param userId The user to be given trusted status
     * @param connectionType The connection type via which the user will be trusted
     * @param ipAddress The ip addresses (as string) from which users
     * will be trusted. This should be an empty string if the connectionType is local.
     */
    @DELETE(BASE + ID_PATH + USER_ID_PATH + CONNECTION_TYPE_PATH + EXTERNAL_DATABASE)
    fun removeHBARecord(
            @Path(ID) organizationId: UUID,
            @Path(USER_ID) userId: String,
            @Path(CONNECTION_TYPE) connectionType: PostgresConnectionType,
            @Body ipAddress: String)

    //get

    /**
     * Gets all OrganizationExternalDatabaseTable objects for an organization
     * @param organizationId The organization's UUID
     */
    @GET(BASE + ID_PATH + EXTERNAL_DATABASE_TABLE)
    fun getExternalDatabaseTables(@Path(ID) organizationId: UUID): Set<ExternalTable>

    /**
     * Gets a map of all OrganizationExternalDatabaseTable objects to
     * OrganizationExternalDatabase columns that are contained within each table.
     * @param organizationId The organization's UUID
     */
    @GET(BASE + ID_PATH + EXTERNAL_DATABASE_TABLE + EXTERNAL_DATABASE_COLUMN)
    fun getExternalDatabaseTablesWithColumnMetadata(@Path(ID) organizationId: UUID): Set<ExternalTableColumnsPair>

    /**
     * Gets a map of all OrganizationExternalDatabaseTable objects to
     * OrganizationExternalDatabase columns that are contained within each table
     * based on the specified permission
     * @param organizationId The organization's UUID
     * @param permission The permission used to filter results
     */
    @GET(BASE + ID_PATH + PERMISSION_PATH + EXTERNAL_DATABASE_TABLE + EXTERNAL_DATABASE_COLUMN + AUTHORIZED)
    fun getAuthorizedExternalDbTablesWithColumnMetadata(
            @Path(ID) organizationId: UUID,
            @Path(PERMISSION) permission: Permission
    ): Set<ExternalTableColumnsPair>

    /**
     * Gets an object containing an OrganizationExternalDatabaseTable
     * object and its OrganizationExternalDatabase columns
     * for an organization
     * @param organizationId The organization's UUID
     * @param tableId The id of the organization's table
     */
    @GET(BASE + ID_PATH + TABLE_ID_PATH + EXTERNAL_DATABASE_TABLE + EXTERNAL_DATABASE_COLUMN)
    fun getExternalDatabaseTableWithColumnMetadata(
            @Path(ID) organizationId: UUID,
            @Path(TABLE_ID) tableId: UUID
    ): ExternalTableColumnsPair

    /**
     * Gets an OrganizationExternalDatabaseTable object with
     * user specified number of rows of raw data for an organization
     * @param organizationId The organization's UUID
     * @param tableId The id of the organization's table
     * @param rowCount The number of rows to be retrieved
     * @return a map of column id to a descending list of all data in that column.
     * Empty values in postgres are nulls in the list.
     */
    @GET(BASE + ID_PATH + TABLE_ID_PATH + ROW_COUNT_PATH + DATA)
    fun getExternalDatabaseTableData(
            @Path(ID) organizationId: UUID,
            @Path(TABLE_ID) tableId: UUID,
            @Path(ROW_COUNT) rowCount: Int
    ): Map<UUID, List<Any?>>

    /**
     * Gets an OrganizationExternalDatabaseTable object, which represents an
     * organization's table in an external database
     * @param organizationId The organization's UUID
     * @param tableId The id of the organization's table
     */
    @GET(BASE + ID_PATH + TABLE_ID_PATH + EXTERNAL_DATABASE_TABLE)
    fun getExternalDatabaseTable(
            @Path(ID) organizationId: UUID,
            @Path(TABLE_ID) tableId: UUID
    ): ExternalTable

    @GET(BASE + ID_PATH + TABLE_ID_PATH + SCHEMA_PATH)
    fun getExternalDatabaseTableSchema(@Path(ID) organizationId: UUID, @Path(TABLE_ID) tableId: UUID): String?

    /**
     * Gets an OrganizationExternalDatabaseColumn object, which represents a column
     * within an organization's table in an external database
     * @param organizationId The organization's UUID
     * @param tableName The exact name of the table in the database
     * @param columnName The exact name of the column in the database
     */
    @GET(BASE + ID_PATH + TABLE_NAME_PATH + COLUMN_NAME_PATH + EXTERNAL_DATABASE_COLUMN)
    fun getExternalDatabaseColumn(
            @Path(ID) organizationId: UUID,
            @Path(TABLE_NAME) tableName: String,
            @Path(COLUMN_NAME) columnName: String
    ): ExternalColumn

    //update

    /**
     * Updates an OrganizationExternalDatabaseTable object's fields that are included
     * within a [MetadataUpdate]
     * @param organizationId The organization's UUID
     * @param tableName The exact name of the table in the database
     * @param metadataUpdate The object used to specify update values
     */
    @PATCH(BASE + ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_TABLE)
    fun updateExternalDatabaseTable(
            @Path(ID) organizationId: UUID,
            @Path(TABLE_NAME) tableName: String,
            @Body metadataUpdate: MetadataUpdate)


    /**
     * Updates an OrganizationExternalDatabaseTable object's fields that are included
     * within a [MetadataUpdate]
     * @param organizationId The organization's UUID
     * @param tableName The exact name of the table in the database
     * @param columnName The exact name of the column in the database
     * @param metadataUpdate The object used to specify update values
     */
    @PATCH(BASE + ID_PATH + TABLE_NAME_PATH + COLUMN_NAME_PATH + EXTERNAL_DATABASE_COLUMN)
    fun updateExternalDatabaseColumn(
            @Path(ID) organizationId: UUID,
            @Path(TABLE_NAME) tableName: String,
            @Path(COLUMN_NAME) columnName: String,
            @Body metadataUpdate: MetadataUpdate)

    //delete
    /**
     * Deletes an OrganizationExternalDatabaseTable object, which represents an
     * organization's table in an external database. This deletes both the object
     * and the table in the database. It is a hard delete
     * @param organizationId The organization's UUID
     * @param tableName The exact name of the table in the database
     */
    @DELETE(BASE + ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_TABLE)
    fun deleteExternalDatabaseTable(
            @Path(ID) organizationId: UUID,
            @Path(TABLE_NAME) tableName: String)

    /**
     * Deletes an OrganizationExternalDatabaseColumn object, which represents a column
     * within an organization's table in an external database. This deletes both the object
     * and the column in the database. It is a hard delete
     * @param organizationId The organization's UUID
     * @param tableName The exact name of the table in the database
     * @param columnName The exact name of the column in the database
     */
    @DELETE(BASE + ID_PATH + TABLE_NAME_PATH + COLUMN_NAME_PATH + EXTERNAL_DATABASE_COLUMN)
    fun deleteExternalDatabaseColumn(
            @Path(ID) organizationId: UUID,
            @Path(TABLE_NAME) tableName: String,
            @Path(COLUMN_NAME) columnName: String)

    /**
     * Deletes multiple OrganizationExternalDatabaseTable objects and the tables they represent
     * in the database. It is a hard delete
     * @param organizationId The organization's UUID
     * @param tableNames The exact names of the tables in the database
     */
    @DELETE(BASE + ID_PATH + EXTERNAL_DATABASE_TABLE)
    fun deleteExternalDatabaseTables(
            @Path(ID) organizationId: UUID,
            @Body tableNames: Set<String>)

    /**
     * Deletes multiple OrganizationExternalDatabaseColumn objects and the columns they represent
     * within an organization's table in an external database. It is a hard delete
     * @param organizationId The organization's UUID
     * @param tableName The exact name of the table in the database
     * @param columnNames The exact names of the columns in the database
     */
    @DELETE(BASE + ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_COLUMN)
    fun deleteExternalDatabaseColumns(
            @Path(ID) organizationId: UUID,
            @Path(TABLE_NAME) tableName: String,
            @Body columnNames: Set<String>)
}
