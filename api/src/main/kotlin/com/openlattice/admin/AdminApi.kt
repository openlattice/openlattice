package com.openlattice.admin

import com.openlattice.authorization.Principal
import retrofit2.http.GET
import retrofit2.http.Path


// @formatter:off
const val SERVICE = "/datastore"
const val CONTROLLER = "/admin"
const val BASE = com.openlattice.auditing.SERVICE + CONTROLLER
// @formatter:on

const val RELOAD_CACHE = "/reload/cache"
const val PRINCIPALS = "/principals"

const val ID = "id"
const val ID_PATH = "/{$ID}"
const val NAME = "name"
const val NAME_PATH = "/{$NAME}"

interface AdminApi {


    /**
     * Retrieve all the events record for a particular entity set in a given time window.
     * @param auditEntitySet The
     */
    @GET(BASE + RELOAD_CACHE)
    fun reloadCache()

    @GET(BASE + RELOAD_CACHE + NAME_PATH)
    fun reloadCache(@Path(NAME) name: String)

    @GET( BASE + PRINCIPALS + ID_PATH )
    fun getUserPrincipals( @Path(ID) principalId :String ): Set<Principal>

}