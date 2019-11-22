package com.openlattice.admin

import com.openlattice.authorization.Principal
import com.openlattice.notifications.sms.SmsEntitySetInformation
import retrofit2.http.*
import java.util.*


// @formatter:off
const val SERVICE = "/datastore"
const val CONTROLLER = "/admin"
const val BASE = SERVICE + CONTROLLER
// @formatter:on

const val RELOAD_CACHE = "/reload/cache"
const val PRINCIPALS = "/principals"
const val SQL = "/sql"
const val LINKING = "linking"
const val OMIT_ENTITY_SET_ID = "omitEntitySetId"
const val ENTITY_SETS = "/entity/sets"
const val COUNT = "/count"
const val PHONE = "/phone"

const val ID = "id"
const val ID_PATH = "/{${ID}}"
const val NAME = "name"
const val NAME_PATH = "/{${NAME}}"

interface AdminApi {


    /**
     * Reload the all the in memory caches.
     */
    @GET(BASE + RELOAD_CACHE)
    fun reloadCache()

    @GET(BASE + RELOAD_CACHE + NAME_PATH)
    fun reloadCache(@Path(NAME) name: String)

    @GET(BASE + PRINCIPALS + ID_PATH)
    fun getUserPrincipals(@Path(ID) principalId: String): Set<Principal>

    @GET(BASE + SQL + ID_PATH)
    fun getEntitySetSql(@Path(ID) entitySetId: UUID, @Query(OMIT_ENTITY_SET_ID) omitEntitySetId: Boolean): String

    @POST(BASE + SQL)
    fun getEntitySetSql(
            @Body entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            @Query(LINKING) linking: Boolean,
            @Query(OMIT_ENTITY_SET_ID) omitEntitySetId: Boolean
    ): String

    @POST(BASE + ENTITY_SETS + COUNT)
    fun countEntitySetsOfEntityTypes(@Body entityTypeIds: Set<UUID>): Map<UUID, Long>

    /**
     * Sets the organization phone number.
     *
     * @param organizationId The organization id to set the phone number for.
     * @param entitySetInformationList An array of [SmsEntitySetInformation] containing per entity set contact info.
     * @return The current phone number after the set operation completed. This be different from the input phone number
     * either because it has been reformatted or someone else set the phone number simultaneously.
     */
    @POST(BASE + ID_PATH + PHONE)
    fun setOrganizationEntitySetInformation(
            @Path(ID) organizationId: UUID,
            @Body entitySetInformationList: List<SmsEntitySetInformation>
    ): Int?

}