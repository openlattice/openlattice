package com.openlattice.search;

import com.openlattice.search.requests.PersistentSearch;
import com.openlattice.search.requests.SearchConstraints;
import retrofit2.http.*;

import java.time.OffsetDateTime;
import java.util.UUID;

public interface PersistentSearchApi {

    String SERVICE    = "/datastore";
    String CONTROLLER = "/persistentsearch";
    String BASE       = SERVICE + CONTROLLER;

    String ID         = "id";
    String EXPIRATION = "/expiration";

    String ID_PATH         = "/{" + ID + "}";
    String EXPIRATION_PATH = "/{" + EXPIRATION + "}";

    String INCLUDE_EXPIRED = "includeExpired";

    @POST( BASE )
    UUID createPersistentSearch( @Body PersistentSearch search );

    @GET( BASE )
    Iterable<PersistentSearch> loadPersistentSearches( @Query( INCLUDE_EXPIRED ) boolean includeExpired );

    @PATCH( BASE + ID_PATH + EXPIRATION )
    Void updatePersistentSearchExpiration( @Path( ID ) UUID id, @Body OffsetDateTime expiration );

    @PATCH( BASE + ID_PATH )
    Void updatePersistentSearchConstraints( @Path( ID ) UUID id, @Body SearchConstraints constraints );

    @DELETE( BASE + ID_PATH )
    Void expirePersistentSearch( @Path( ID ) UUID id );
}
