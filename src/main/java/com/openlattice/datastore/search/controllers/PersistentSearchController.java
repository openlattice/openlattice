package com.openlattice.datastore.search.controllers;

import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Principals;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.search.PersistentSearchApi;
import com.openlattice.search.PersistentSearchService;
import com.openlattice.search.requests.PersistentSearch;
import com.openlattice.search.requests.SearchConstraints;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import java.time.OffsetDateTime;
import java.util.UUID;

@RestController
@RequestMapping( PersistentSearchApi.CONTROLLER )
public class PersistentSearchController implements PersistentSearchApi {

    @Inject
    private PersistentSearchService persistentSearchService;

    @Inject
    private SecurePrincipalsManager spm;

    @Override
    @RequestMapping(
            method = RequestMethod.POST,
            consumes = { MediaType.APPLICATION_JSON_VALUE } )
    public UUID createPersistentSearch( @RequestBody PersistentSearch search ) {
        return persistentSearchService.createPersistentSearch( search );
    }

    @Override
    @RequestMapping(
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    public Iterable<PersistentSearch> loadPersistentSearches(
            @RequestParam( INCLUDE_EXPIRED ) boolean includeExpired ) {
        return persistentSearchService.loadPersistentSearchesForUser( includeExpired );
    }

    @Override
    @RequestMapping(
            path = { ID_PATH + EXPIRATION },
            method = RequestMethod.PATCH )
    public Void updatePersistentSearchExpiration(
            @PathVariable( ID ) UUID id,
            @RequestBody OffsetDateTime expiration ) {
        persistentSearchService.updatePersistentSearchExpiration( id, expiration );
        return null;
    }

    @Override
    @RequestMapping(
            path = { ID_PATH },
            method = RequestMethod.PATCH )
    public Void updatePersistentSearchConstraints(
            @PathVariable( ID ) UUID id,
            @RequestBody SearchConstraints searchConstraints ) {
        persistentSearchService.updatePersistentSearchConstraints( id, searchConstraints );
        return null;
    }

    @Override
    @RequestMapping(
            path = { ID_PATH },
            method = RequestMethod.DELETE )
    public Void expirePersistentSearch( @PathVariable( ID ) UUID id ) {
        persistentSearchService.updatePersistentSearchExpiration( id, OffsetDateTime.now() );
        return null;
    }
}
