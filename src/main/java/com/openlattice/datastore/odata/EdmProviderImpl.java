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

package com.openlattice.datastore.odata;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.openlattice.datastore.odata.Transformers.EntitySetTransformer;
import com.openlattice.datastore.odata.Transformers.EntityTypeTransformer;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.Schema;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.edm.type.EntityType;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlAbstractEdmProvider;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.ex.ODataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Entity Data Model provider.
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 *
 */
public class EdmProviderImpl extends CsdlAbstractEdmProvider {
    private static final Logger          logger         = LoggerFactory
            .getLogger( EdmProviderImpl.class );
    public static final String           NAMESPACE      = "OData.Demo";
    public final String                  CONTAINER_NAME = "Container";
    public final FullQualifiedName       CONTAINER      = new FullQualifiedName(
            NAMESPACE,
            CONTAINER_NAME );
    private final EdmManager             dms;
    private final HazelcastSchemaManager schemaManager;
    private final EntityTypeTransformer  ett;
    private final EntitySetTransformer   est;

    public EdmProviderImpl( EdmManager dms, HazelcastSchemaManager schemaManager ) {
        this.dms = dms;
        this.schemaManager = schemaManager;
        this.ett = new EntityTypeTransformer( dms );
        this.est = new EntitySetTransformer( dms );
    }

    @Override
    public CsdlEntityType getEntityType( FullQualifiedName entityTypeName ) throws ODataException {
        EntityType objectType = dms.getEntityType( entityTypeName.getNamespace(), entityTypeName.getName() );

        return ett.transform( objectType );
    }

    public CsdlEntitySet getEntitySet( FullQualifiedName entityContainer, String entitySetName ) {
        EntitySet entitySet = dms.getEntitySet( entitySetName );
        return est.transform( entitySet );
    }

    public CsdlEntityContainer getEntityContainer() {
        // create EntitySets
        List<CsdlEntitySet> entitySets = Lists.newArrayList( Iterables
                .filter( Iterables.transform( dms.getEntitySets(), est::transform ), Predicates.notNull() ) );

        // create EntityContainer
        CsdlEntityContainer entityContainer = new CsdlEntityContainer();
        entityContainer.setName( CONTAINER_NAME );
        entityContainer.setEntitySets( entitySets );

        return entityContainer;
    }

    public List<CsdlSchema> getSchemas() throws ODataException {
        List<CsdlSchema> schemas = new ArrayList<CsdlSchema>();

        for ( Schema schemaMetadata : schemaManager.getAllSchemas() ) {
            CsdlSchema schema = new CsdlSchema();
            String namespace = schemaMetadata.getFqn().getNamespace();
            schema.setNamespace( namespace );
            List<CsdlEntityType> entityTypes = schemaMetadata.getEntityTypes().parallelStream()
                    .map( fqn -> {
                        try {
                            return getEntityType( fqn.getType() );
                        } catch ( ODataException e ) {
                            logger.error( "Unable to get entity type for FQN={}", fqn );
                            return null;
                        }
                    } )
                    .filter( et -> et != null )
                    .collect( Collectors.toList() );
            schema.setEntityTypes( entityTypes );
            schema.setEntityContainer( getEntityContainer() );
            schemas.add( schema );
        }

        return schemas;
    }

    public CsdlEntityContainerInfo getEntityContainerInfo( FullQualifiedName entityContainerName ) {

        // This method is invoked when displaying the Service Document at e.g.
        // http://localhost:8080/DemoService/DemoService.svc
        if ( entityContainerName == null || entityContainerName.equals( CONTAINER ) ) {
            CsdlEntityContainerInfo entityContainerInfo = new CsdlEntityContainerInfo();
            entityContainerInfo.setContainerName( CONTAINER );
            return entityContainerInfo;
        }

        return null;
    }
}
