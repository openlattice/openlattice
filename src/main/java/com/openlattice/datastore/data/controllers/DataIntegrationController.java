/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.datastore.data.controllers;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.SetMultimap;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.EdmAuthorizationHelper;
import com.openlattice.authorization.Permission;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DataIntegrationApi;
import com.openlattice.data.IntegrationResults;
import com.openlattice.data.integration.Association;
import com.openlattice.data.integration.BulkDataCreation;
import com.openlattice.data.integration.Entity;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.SearchService;
import com.openlattice.edm.type.PropertyType;

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(DataIntegrationApi.CONTROLLER)
public class DataIntegrationController implements DataIntegrationApi, AuthorizingComponent {
    private static final Logger logger = LoggerFactory
            .getLogger(DataIntegrationController.class);
    private static final EnumSet<Permission> WRITE_PERMISSION = EnumSet.of(Permission.WRITE);
    @Inject
    private EdmService dms;
    @Inject
    private DataGraphManager dgm;
    @Inject
    private AuthorizationManager authz;
    @Inject
    private EdmAuthorizationHelper authzHelper;
    @Inject
    private AuthenticationManager authProvider;
    @Inject
    private SearchService searchService;
    private LoadingCache<UUID, EdmPrimitiveTypeKind> primitiveTypeKinds;
    private LoadingCache<AuthorizationKey, Set<UUID>> authorizedPropertyCache;

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authz;
    }

    @PostMapping("/" + ENTITY_SET + "/" + SET_ID_PATH)
    @Override
    public IntegrationResults integrateEntities(
            @PathVariable(ENTITY_SET_ID) UUID entitySetId,
            @RequestParam(value = DETAILED_RESULTS, required = false, defaultValue = "false") boolean detailedResults,
            @RequestBody Map<String, Map<UUID, Set<Object>>> entities) {
        //Ensure that we have read access to entity set metadata.
        ensureReadAccess(new AclKey(entitySetId));
        //Load authorized property types
        final Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes(entitySetId, WRITE_PERMISSION);

        final Map<String, UUID> entityKeyIds = dgm.integrateEntities(entitySetId, entities, authorizedPropertyTypes);
        final IntegrationResults results = new IntegrationResults(entityKeyIds.size(),
                0,
                detailedResults ? Optional.of(ImmutableMap.of(entitySetId, entityKeyIds)) : Optional.empty(),
                Optional.empty());
        return results;
    }

    @PostMapping("/" + ASSOCIATION + "/" + SET_ID_PATH)
    @Override
    public IntegrationResults integrateAssociations(
            @RequestBody Set<Association> associations,
            @RequestParam(value = DETAILED_RESULTS, required = false, defaultValue = "false")
                    boolean detailedResults) {
        final Set<UUID> associationEntitySets = associations.stream()
                .map(association -> association.getKey().getEntitySetId())
                .collect(Collectors.toSet());
        //Ensure that we have read access to entity set metadata.
        associationEntitySets.forEach(entitySetId -> ensureReadAccess(new AclKey(entitySetId)));
        accessCheck(EdmAuthorizationHelper
                .aclKeysForAccessCheck(requiredAssociationPropertyTypes(associations), WRITE_PERMISSION));

        final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySet =
                associationEntitySets.stream()
                        .collect(Collectors.toMap(Function.identity(),
                                entitySetId -> authzHelper
                                        .getAuthorizedPropertyTypes(entitySetId, EnumSet.of(Permission.WRITE))));

        //        checkAllAssociationsInEntitySet( entitySetId, associations );
        final Map<UUID, Map<String, UUID>> entityKeyIds = dgm
                .integrateAssociations(associations, authorizedPropertyTypesByEntitySet);
        final IntegrationResults results = new IntegrationResults(0,
                entityKeyIds.values().stream().mapToInt(Map::size).sum(),
                Optional.empty(),
                detailedResults ? Optional.of(entityKeyIds) : Optional.empty());
        return results;
    }

    @PostMapping({"/", ""})
    @Override
    public IntegrationResults integrateEntityAndAssociationData(
            @RequestBody BulkDataCreation data,
            @RequestParam(value = DETAILED_RESULTS, required = false, defaultValue = "false")
                    boolean detailedResults) {
        final Set<Entity> entities = data.getEntities();
        final Set<Association> associations = data.getAssociations();

        final Set<UUID> entitySets = entities.stream()
                .map(entity -> entity.getKey().getEntitySetId())
                .collect(Collectors.toSet());

        final Set<UUID> associationEntitySets = associations.stream()
                .map(association -> association.getKey().getEntitySetId())
                .collect(Collectors.toSet());

        //Ensure that we have read access to entity set metadata.
        entitySets.forEach(entitySetId -> ensureReadAccess(new AclKey(entitySetId)));
        associationEntitySets.forEach(entitySetId -> ensureReadAccess(new AclKey(entitySetId)));

        accessCheck(EdmAuthorizationHelper
                .aclKeysForAccessCheck(requiredEntityPropertyTypes(entities), WRITE_PERMISSION));
        accessCheck(EdmAuthorizationHelper
                .aclKeysForAccessCheck(requiredAssociationPropertyTypes(associations), WRITE_PERMISSION));

        final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySet =
                Stream.concat(entitySets.stream(), associationEntitySets.stream())
                        .collect(Collectors.toMap(Function.identity(),
                                entitySetId -> authzHelper
                                        .getAuthorizedPropertyTypes(entitySetId, WRITE_PERMISSION)));

        return dgm.integrateEntitiesAndAssociations(data.getEntities(),
                data.getAssociations(),
                authorizedPropertyTypesByEntitySet);
    }

    private static SetMultimap<UUID, UUID> requiredAssociationPropertyTypes(Set<Association> associations) {
        final SetMultimap<UUID, UUID> propertyTypesByEntitySet = HashMultimap.create();
        associations.forEach(association -> propertyTypesByEntitySet
                .putAll(association.getKey().getEntitySetId(), association.getDetails().keySet()));
        return propertyTypesByEntitySet;
    }

    private static SetMultimap<UUID, UUID> requiredEntityPropertyTypes(Set<Entity> entities) {
        final SetMultimap<UUID, UUID> propertyTypesByEntitySet = HashMultimap.create();
        entities.forEach(entity -> propertyTypesByEntitySet
                .putAll(entity.getEntitySetId(), entity.getDetails().keySet()));
        return propertyTypesByEntitySet;
    }

}
