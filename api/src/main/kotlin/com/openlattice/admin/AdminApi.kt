package com.openlattice.admin

import com.openlattice.authorization.Principal
import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.Path
import java.util.*


// @formatter:off
const val SERVICE = "/datastore"
const val CONTROLLER = "/admin"
const val BASE = SERVICE + CONTROLLER
// @formatter:on

const val RELOAD_CACHE = "/reload/cache"
const val PRINCIPALS = "/principals"
const val ENTITY_SETS = "/entity/sets"
const val COUNT = "/count"

const val ID = "id"
const val ID_PATH = "/{${ID}}"
const val NAME = "name"
const val NAME_PATH = "/{${NAME}}"

interface AdminApi {


    /**
     * Reload the all the in meory caches.
     */
    @GET(BASE + RELOAD_CACHE)
    fun reloadCache()

    @GET(BASE + RELOAD_CACHE + NAME_PATH)
    fun reloadCache(@Path(NAME) name: String)

    @GET(BASE + PRINCIPALS + ID_PATH)
    fun getUserPrincipals(@Path(ID) principalId: String): Set<Principal>

    @POST(BASE + ENTITY_SETS + COUNT)
    fun countEntitySetsOfEntityTypes(@Body entityTypeIds: Set<UUID>) :Map<UUID, Long>

}