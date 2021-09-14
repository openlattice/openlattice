package com.openlattice.admin

import com.geekbeast.rhizome.jobs.DistributableJob
import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.authorization.Principal
import com.openlattice.jobs.JobUpdate
import com.openlattice.notifications.sms.SmsEntitySetInformation
import com.openlattice.organizations.Organization
import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.PATCH
import retrofit2.http.POST
import retrofit2.http.Path
import retrofit2.http.Query
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
const val ORGANIZATION = "/organization"
const val USAGE = "/usage"

const val ID = "id"
const val ID_PATH = "/{$ID}"
const val NAME = "name"
const val NAME_PATH = "/{$NAME}"

const val JOBS = "/jobs"

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

    @GET(BASE + ORGANIZATION + USAGE)
    fun getEntityCountByOrganization(): Map<UUID, Long>

    @GET(BASE + ORGANIZATION)
    fun getAllOrganizations(): Iterable<Organization>

    @GET(BASE + JOBS)
    fun getJobs(): Map<UUID, DistributableJob<*>>

    @POST(BASE + JOBS)
    fun getJobs(@Body statuses: Set<JobStatus>): Map<UUID, DistributableJob<*>>

    @GET(BASE + JOBS + ID_PATH)
    fun getJob(@Path(ID) jobId: UUID): Map<UUID, DistributableJob<*>>

    @PATCH(BASE + JOBS)
    fun createJobs(@Body jobs: List<DistributableJob<*>>): List<UUID>

    @PATCH(BASE + JOBS + ID_PATH)
    fun updateJob(@Path(ID) jobId: UUID, @Body update: JobUpdate): Map<UUID, DistributableJob<*>>
}