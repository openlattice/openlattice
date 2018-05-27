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

package com.openlattice.data;

import com.google.common.collect.SetMultimap;
import com.openlattice.analysis.requests.TopUtilizerDetails;
import com.openlattice.data.requests.Association;
import com.openlattice.data.requests.Entity;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.graph.core.objects.NeighborTripletSet;
import com.openlattice.graph.edge.EdgeKey;
import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

public interface DataGraphManager {

    /*
     * Entity set methods
     */
    EntitySetData<FullQualifiedName> getEntitySetData(
            UUID entitySetId,
            LinkedHashSet<String> orderedPropertyNames,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    /*
     * CRUD methods for entity
     */
    SetMultimap<FullQualifiedName, Object> getEntity(
            UUID entityKeyId,
            UUID entitySetId,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    //Soft deletes
    int clearEntitySet( UUID entitySetId, Map<UUID, PropertyType> authorizedPropertyTypes );

    int clearEntities( UUID entitySetId, Set<UUID> entityKeyIds, Map<UUID, PropertyType> authorizedPropertyTypes );

    int clearAssociations( Set<EdgeKey> key );

    //Hard deletes
    int deleteEntitySet( UUID entitySetId, Map<UUID, PropertyType> authorizedPropertyTypes );

    int deleteEntities( UUID entitySetId, Set<UUID> entityKeyIds, Map<UUID, PropertyType> authorizedPropertyTypes );

    int deleteAssociation( Set<EdgeKey> key, Map<UUID, PropertyType> authorizedPropertyTypes );

    /*
     * Bulk endpoints for entities/associations
     */

    Map<String, UUID> createEntities(
            UUID entitySetId,
            Map<String, SetMultimap<UUID, Object>> entities,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    void replaceEntites(
            UUID entitySetId,
            Map<UUID, SetMultimap<UUID, Object>> entities,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    void partialReplaceEntites(
            UUID entitySetId,
            Map<UUID, SetMultimap<UUID, Object>> entities,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    void replacePropertiesInEntities(
            UUID entitySetId,
            Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Object>>> replacementProperties,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    void createAssociations(
            Set<Association> associations,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes );

    void createEntitiesAndAssociations(
            Set<Entity> entities,
            Set<Association> associations,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertiesByEntitySetId );

    Stream<SetMultimap<FullQualifiedName, Object>> getTopUtilizers(
            UUID entitySetId,
            List<TopUtilizerDetails> topUtilizerDetails,
            int numResults,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    NeighborTripletSet getNeighborEntitySets( UUID entitySetId );
}
