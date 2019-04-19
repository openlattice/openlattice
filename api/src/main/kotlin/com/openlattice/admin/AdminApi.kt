package com.openlattice.admin

import com.openlattice.authorization.Principal
import retrofit2.http.GET
import retrofit2.http.Path


// @formatter:off
const val SERVICE = "/datastore"
const val CONTROLLER = "/admin"
const val BASE = com.openlattice.admin.indexing.SERVICE + com.openlattice.admin.indexing.CONTROLLER
// @formatter:on

const val RELOAD_CACHE = "/reload/cache"
const val PRINCIPALS = "/principals"

const val ID = "id"
const val ID_PATH = "/{${com.openlattice.admin.indexing.ID}}"
const val NAME = "name"
const val NAME_PATH = "/{${com.openlattice.admin.indexing.NAME}}"

interface AdminApi {


    /**
     * Retrieve all the events record for a particular entity set in a given time window.
     * @param auditEntitySet The
     */
    @GET(com.openlattice.admin.indexing.BASE + RELOAD_CACHE)
    fun reloadCache()

    @GET(com.openlattice.admin.indexing.BASE + RELOAD_CACHE + com.openlattice.admin.indexing.NAME_PATH)
    fun reloadCache(@Path(com.openlattice.admin.indexing.NAME) name: String)

    @GET(com.openlattice.admin.indexing.BASE + com.openlattice.admin.indexing.PRINCIPALS + com.openlattice.admin.indexing.ID_PATH)
    fun getUserPrincipals( @Path(com.openlattice.admin.indexing.ID) principalId :String ): Set<Principal>

}