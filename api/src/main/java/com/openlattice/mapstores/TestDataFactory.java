/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.mapstores;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.openlattice.IdConstants;
import com.openlattice.apps.App;
import com.openlattice.apps.AppConfigKey;
import com.openlattice.apps.AppRole;
import com.openlattice.apps.AppTypeSetting;
import com.openlattice.authorization.*;
import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.authorization.securable.AbstractSecurableType;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.codex.Base64Media;
import com.openlattice.codex.MessageRequest;
import com.openlattice.collections.CollectionTemplateType;
import com.openlattice.collections.EntitySetCollection;
import com.openlattice.collections.EntityTypeCollection;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityKey;
import com.openlattice.edm.EdmDetails;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.set.EntitySetFlag;
import com.openlattice.edm.type.Analyzer;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.EntityTypePropertyMetadata;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.notifications.sms.SmsEntitySetInformation;
import com.openlattice.organization.OrganizationExternalDatabaseColumn;
import com.openlattice.organization.OrganizationExternalDatabaseTable;
import com.openlattice.organization.OrganizationPrincipal;
import com.openlattice.organization.roles.Role;
import com.openlattice.organizations.Grant;
import com.openlattice.organizations.GrantType;
import com.openlattice.organizations.Organization;
import com.openlattice.organizations.OrganizationDatabase;
import com.openlattice.organizations.OrganizationMetadataEntitySetIds;
import com.openlattice.postgres.IndexType;
import com.openlattice.postgres.PostgresAuthenticationRecord;
import com.openlattice.postgres.PostgresConnectionType;
import com.openlattice.postgres.PostgresDatatype;
import com.openlattice.requests.PermissionsRequestDetails;
import com.openlattice.requests.Request;
import com.openlattice.requests.RequestStatus;
import com.openlattice.requests.Status;
import com.openlattice.search.PersistentSearchNotificationType;
import com.openlattice.search.requests.PersistentSearch;
import com.openlattice.search.requests.SearchConstraints;
import com.openlattice.search.requests.SearchDetails;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.text.CharacterPredicates;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.jetbrains.annotations.NotNull;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressFBWarnings( value = "SECPR", justification = "Only used for testing." )
public final class TestDataFactory {
    private static final GrantType[]           grants                  = GrantType.values();
    private static final SecurableObjectType[] securableObjectTypes    = SecurableObjectType.values();
    private static final Permission[]          permissions             = Permission.values();
    private static final Action[]              actions                 = Action.values();
    private static final RequestStatus[]       requestStatuses         = RequestStatus.values();
    private static final Analyzer[]            analyzers               = Analyzer.values();
    private static final EntitySetFlag[]       entitySetFlags          = EntitySetFlag.values();
    private static final IndexType[]           INDEX_TYPES             = IndexType.values();
    private static final Random                r                       = new Random();
    private static final char[][]              allowedLetters          = { { 'a', 'z' }, { 'A', 'Z' } };
    private static final char[][]              allowedDigitsAndLetters = { { 'a', 'z' }, { 'A', 'Z' }, { '0', '9' } };
    private static final RandomStringGenerator random                  = new RandomStringGenerator.Builder()
            .build();
    private static final RandomStringGenerator randomAlpha             = new RandomStringGenerator.Builder()
            .withinRange( allowedLetters )
            .filteredBy( CharacterPredicates.LETTERS )
            .build();
    private static final RandomStringGenerator randomAlphaNumeric      = new RandomStringGenerator.Builder()
            .withinRange( allowedDigitsAndLetters )
            .filteredBy( CharacterPredicates.LETTERS, CharacterPredicates.DIGITS )
            .build();

    private TestDataFactory() {
    }

    @NotNull public static Map<UUID, Map<UUID, Set<Object>>> entities(
            int numEntities,
            @NotNull Map<UUID, PropertyType> propertyTypes ) {
        final Map<UUID, Map<UUID, Set<Object>>> entities = new HashMap<>( numEntities );

        for ( int i = 0; i < numEntities; ++i ) {
            final var properties = Maps.newHashMap( propertyTypes.
                    values()
                    .stream()
                    .collect( Collectors.toMap( PropertyType::getId, TestDataFactory::randomElements ) ) );
            final var id = UUID.randomUUID();
            properties.put( IdConstants.ID_ID.getId(), Sets.newHashSet( id.toString() ) );
            properties.put( IdConstants.LAST_WRITE_ID.getId(), Sets.newHashSet( OffsetDateTime.now() ) );
            properties.put( IdConstants.VERSION_ID.getId(), Sets.newHashSet( System.currentTimeMillis() ) );

            entities.put( UUID.randomUUID(), properties );
        }
        return entities;
    }

    public static Set<Object> randomElements( PropertyType pt ) {
        final var count = 1 + r.nextInt( 5 );
        final var elements = new HashSet<>( count );
        for ( int i = 0; i < count; ++i ) {
            elements.add( randomElement( pt ) );
        }
        return elements;
    }

    public static Object randomElement( PropertyType pt ) {
        switch ( pt.getDatatype() ) {
            case Int64:
                return r.nextLong();
            case Int32:
                return r.nextInt();
            case Int16:
                return (short) r.nextInt( Short.MAX_VALUE );
            case String:
            default:
                return RandomStringUtils.random( 10 );

        }
    }

    public static EntityDataKey entityDataKey() {
        return new EntityDataKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    public static Long longValue() {
        return r.nextLong();
    }

    public static Integer integer() {
        return r.nextInt();
    }

    public static String random( int length ) {
        return random.generate( length );
    }

    public static String randomAlphabetic( int length ) {
        return randomAlpha.generate( length );
    }

    public static String randomAlphanumeric( int length ) {
        return randomAlphaNumeric.generate( length );
    }

    public static Principal userPrincipal() {
        return new Principal( PrincipalType.USER, randomAlphanumeric( 10 ) );
    }

    public static Principal rolePrincipal() {
        return new Principal( PrincipalType.ROLE, randomAlphanumeric( 5 ) );
    }

    public static SecurablePrincipal securableUserPrincipal() {
        return securablePrincipal( PrincipalType.USER );
    }

    public static SecurablePrincipal securablePrincipal( PrincipalType type ) {
        // TODO after Java 13: use switch expression
        Principal principal;
        switch ( type ) {
            case ROLE:
                principal = rolePrincipal();
                break;
            case ORGANIZATION:
                principal = organizationPrincipal();
                break;
            case USER:
            default:
                principal = userPrincipal();
        }

        return new SecurablePrincipal(
                new AclKey( UUID.randomUUID() ),
                principal,
                randomAlphanumeric( 10 ),
                Optional.of( randomAlphanumeric( 10 ) )
        );
    }

    public static EntityType entityType( PropertyType... keys ) {
        return childEntityType( null, null, keys );
    }

    public static EntityType entityType( SecurableObjectType category, PropertyType... keys ) {
        return childEntityType( null, category, keys );
    }

    public static EntityType entityType(
            Optional<FullQualifiedName> fqn,
            SecurableObjectType category,
            PropertyType... keys ) {
        return childEntityTypeWithPropertyType(
                null,
                fqn,
                ImmutableSet.of( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() ),
                category,
                keys );
    }

    public static EntityType childEntityType( UUID parentId, SecurableObjectType category, PropertyType... keys ) {
        return childEntityTypeWithPropertyType( parentId,
                Optional.empty(),
                ImmutableSet.of( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() ),
                category,
                keys );
    }

    public static EntityType childEntityTypeWithPropertyType(
            UUID parentId,
            Optional<FullQualifiedName> fqn,
            Set<UUID> propertyTypes,
            SecurableObjectType category,
            PropertyType... keys ) {
        LinkedHashSet<UUID> k = keys.length > 0
                ? Arrays.asList( keys ).stream().map( PropertyType::getId )
                .collect( Collectors.toCollection( Sets::newLinkedHashSet ) )
                : Sets.newLinkedHashSet( Arrays.asList( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() ) );
        var propertyTags = new LinkedHashMap<UUID, LinkedHashSet<String>>( k.size() );

        for ( UUID id : k ) {
            propertyTags.put( id, new LinkedHashSet<>() );
            propertyTags.get( id ).add( "PRIMARY KEY TAG" );
        }

        SecurableObjectType entityTypeCategory = ( category == null ) ? SecurableObjectType.EntityType : category;

        return new EntityType(
                UUID.randomUUID(),
                fqn.orElseGet( TestDataFactory::fqn ),
                randomAlphanumeric( 5 ),
                Optional.of( randomAlphanumeric( 5 ) ),
                ImmutableSet.of( fqn(), fqn(), fqn() ),
                k,
                Sets.newLinkedHashSet( Sets
                        .union( k, propertyTypes ) ),
                propertyTags,
                Optional.ofNullable( parentId ),
                Optional.of( entityTypeCategory ),
                Optional.of( RandomUtils.nextInt( 1, 5 ) ) );
    }

    public static AssociationType associationType( PropertyType... keys ) {
        EntityType et = entityType( SecurableObjectType.AssociationType, keys );
        return new AssociationType(
                Optional.of( et ),
                Sets.newLinkedHashSet( Arrays.asList( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() ) ),
                Sets.newLinkedHashSet( Arrays.asList( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() ) ),
                false );
    }

    public static AssociationType associationTypeWithProperties( Set<UUID> propertyTypes, PropertyType... keys ) {
        if ( propertyTypes.size() == 0 ) { return associationType( keys ); }
        EntityType et = childEntityTypeWithPropertyType(
                null,
                Optional.empty(),
                propertyTypes,
                SecurableObjectType.AssociationType,
                keys );
        UUID ptId = propertyTypes.iterator().next();
        return new AssociationType(
                Optional.of( et ),
                Sets.newLinkedHashSet( Collections.singletonList( ptId ) ),
                Sets.newLinkedHashSet( Collections.singletonList( ptId ) ),
                false );
    }

    public static FullQualifiedName fqn() {
        return new FullQualifiedName(
                randomAlphanumeric( 5 ),
                randomAlphanumeric( 5 ) );
    }

    public static String email() {
        return randomAlphanumeric( 5 ) + "@" + randomAlphanumeric( 5 ) + ".com";
    }

    public static String name() {
        return randomAlphanumeric( 5 );
    }

    public static EntitySet entitySet() {
        return entitySetWithType( UUID.randomUUID() );
    }

    public static EntitySet entitySetWithType( UUID entityTypeId ) {
        return new EntitySet(
                UUID.randomUUID(),
                entityTypeId,
                randomAlphanumeric( 5 ),
                randomAlphanumeric( 5 ),
                randomAlphanumeric( 5 ),
                ImmutableSet.of( email(), email() ),
                IdConstants.GLOBAL_ORGANIZATION_ID.getId() );
    }

    public static PropertyType datePropertyType() {
        return new PropertyType(
                UUID.randomUUID(),
                fqn(),
                randomAlphanumeric( 5 ),
                Optional.of( randomAlphanumeric( 5 ) ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.Date,
                Optional.of( r.nextBoolean() ),
                Optional.of( Analyzer.STANDARD ),
                Optional.of( indexType() ) );
    }

    public static PropertyType dateTimePropertyType() {
        return new PropertyType(
                UUID.randomUUID(),
                fqn(),
                randomAlphanumeric( 5 ),
                Optional.of( randomAlphanumeric( 5 ) ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.DateTimeOffset,
                Optional.of( r.nextBoolean() ),
                Optional.of( Analyzer.STANDARD ),
                Optional.of( indexType() ) );
    }

    public static PropertyType enumType() {
        return propertyType( indexType(), true );
    }

    public static PropertyType propertyType() {
        return propertyType( indexType(), false );
    }

    public static PropertyType propertyType( IndexType postgresIndexType, boolean isEnumType ) {
        Optional<Set<String>> enumValues = isEnumType ?
                Optional.of( Sets.newHashSet( RandomStringUtils.random( 5 ), RandomStringUtils.random( 5 ) ) ) :
                Optional.empty();
        return new PropertyType(
                UUID.randomUUID(),
                fqn(),
                randomAlphanumeric( 5 ),
                Optional.of( randomAlphanumeric( 5 ) ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String,
                enumValues,
                Optional.of( r.nextBoolean() ),
                Optional.of( analyzer() ),
                Optional.of( postgresIndexType ) );
    }

    public static PropertyType binaryPropertyType() {
        return new PropertyType(
                UUID.randomUUID(),
                fqn(),
                randomAlphanumeric( 5 ),
                Optional.of( randomAlphanumeric( 5 ) ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.Binary,
                Optional.of( r.nextBoolean() ),
                Optional.of( analyzer() ),
                Optional.of( IndexType.NONE ) );
    }

    public static PropertyType propertyType( EdmPrimitiveTypeKind type ) {
        switch ( type ) {
            case String:
                return propertyType();
            default:
                return new PropertyType(
                        UUID.randomUUID(),
                        fqn(),
                        randomAlphanumeric( 5 ),
                        Optional.of( randomAlphanumeric( 5 ) ),
                        ImmutableSet.of(),
                        type,
                        Optional.of( r.nextBoolean() ),
                        Optional.empty(),
                        Optional.of( indexType() ) );
        }
    }

    public static Analyzer analyzer() {
        return analyzers[ r.nextInt( analyzers.length ) ];
    }

    public static IndexType indexType() {
        return INDEX_TYPES[ r.nextInt( INDEX_TYPES.length ) ];
    }

    public static Organization organization() {
        final var grant = grant();
        final var orgPrincipal = securableOrganizationPrincipal();

        return new Organization(
                orgPrincipal,
                new AclKey( orgPrincipal.getId(), UUID.randomUUID() ),
                Sets.newHashSet( randomAlphanumeric( 5 ), randomAlphanumeric( 5 ) ),
                Sets.newHashSet( userPrincipal() ),
                Sets.newHashSet( role() ),
                Sets.newHashSet( smsEntitySetInformation() ),
                Lists.newArrayList( 1, 2, 3 ),
                Sets.newHashSet( UUID.randomUUID() ),
                Sets.newHashSet( randomAlphanumeric( 5 ), randomAlphanumeric( 5 ) ),
                Maps.newHashMap( ImmutableMap
                        .of( UUID.randomUUID(), ImmutableMap.of( grant.getGrantType(), grant() ) )
                ),
                new OrganizationMetadataEntitySetIds()
        );
    }

    public static Grant grant() {
        final var grantType = grants[ r.nextInt( grants.length ) ];
        final var emailSet = Sets.newHashSet( "foo@bar.com" );
        final var otherSet = Sets.newHashSet( RandomStringUtils.random( 10 ), RandomStringUtils.random( 10 ) );
        if ( grantType.equals( GrantType.EmailDomain ) ) {
            return new Grant( grantType,
                    emailSet,
                    RandomStringUtils.random( 8 )
            );
        } else {
            return new Grant( grantType,
                    otherSet,
                    RandomStringUtils.random( 8 )
            );
        }
    }

    public static Principal organizationPrincipal() {
        return new Principal( PrincipalType.ORGANIZATION, randomAlphanumeric( 10 ) );
    }

    public static OrganizationPrincipal securableOrganizationPrincipal() {
        return new OrganizationPrincipal(
                Optional.of( UUID.randomUUID() ),
                organizationPrincipal(),
                randomAlphanumeric( 5 ),
                Optional.of( randomAlphanumeric( 10 ) )
        );
    }


    public static Role role() {
        return new Role(
                Optional.of( UUID.randomUUID() ),
                UUID.randomUUID(),
                rolePrincipal(),
                randomAlphanumeric( 5 ),
                Optional.of( randomAlphanumeric( 5 ) ) );
    }

    public static Role role( UUID organizationId ) {
        return new Role(
                Optional.of( UUID.randomUUID() ),
                organizationId,
                rolePrincipal(),
                randomAlphanumeric( 5 ),
                Optional.of( randomAlphanumeric( 5 ) ) );
    }

    public static SecurableObjectType securableObjectType() {
        return securableObjectTypes[ r.nextInt( securableObjectTypes.length ) ];
    }

    public static EnumSet<Permission> permissions() {
        return Arrays.asList( permissions )
                .stream()
                .filter( elem -> r.nextBoolean() )
                .collect( Collectors.toCollection( () -> EnumSet.noneOf( Permission.class ) ) );
    }

    public static EnumSet<Permission> nonEmptyPermissions() {
        EnumSet<Permission> ps = permissions();
        while ( ps.isEmpty() ) {
            ps = permissions();
        }
        return ps;
    }

    public static Ace ace() {
        return new Ace( userPrincipal(), permissions() );
    }

    public static AceValue aceValue() {
        return new AceValue(
                permissions(),
                securableObjectType()
        );
    }

    public static Acl acl() {
        return new Acl(
                ImmutableList.of( UUID.randomUUID(), UUID.randomUUID() ),
                ImmutableList.of( ace(), ace(), ace(), ace() ) );
    }

    public static AclData aclData() {
        return new AclData(
                acl(),
                actions[ r.nextInt( actions.length ) ] );
    }

    public static AclKey aclKey() {
        return new AclKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    public static EdmDetails edmDetails() {
        Set<PropertyType> pts = ImmutableSet.of( propertyType(), propertyType(), propertyType() );
        Set<EntityType> ets = ImmutableSet.of( entityType(), entityType(), entityType() );
        Set<EntitySet> ess = ImmutableSet.of( entitySet() );
        return new EdmDetails(
                pts.stream().collect( Collectors.toMap( AbstractSecurableType::getId, v -> v ) ),
                ets.stream().collect( Collectors.toMap( AbstractSecurableType::getId, v -> v ) ),
                ess.stream().collect( Collectors.toMap( AbstractSecurableObject::getId, v -> v ) ) );
    }

    public static Map<UUID, EnumSet<Permission>> aclChildPermissions() {
        Map<UUID, EnumSet<Permission>> permissions = new HashMap<>();
        permissions.put( UUID.randomUUID(), EnumSet.of( Permission.READ ) );
        permissions.put( UUID.randomUUID(), EnumSet.of( Permission.WRITE ) );
        permissions.put( UUID.randomUUID(), EnumSet.of( Permission.READ, Permission.WRITE ) );
        return permissions;
    }

    public static PermissionsRequestDetails unresolvedPRDetails() {
        return new PermissionsRequestDetails( aclChildPermissions(), RequestStatus.SUBMITTED );
    }

    public static PermissionsRequestDetails resolvedPRDetails() {
        return new PermissionsRequestDetails( aclChildPermissions(), RequestStatus.APPROVED );
    }

    public static RequestStatus requestStatus() {
        return requestStatuses[ r.nextInt( requestStatuses.length ) ];
    }

    public static Request request() {
        return new Request(
                TestDataFactory.aclKey(),
                TestDataFactory.permissions(),
                Optional.of( "Requesting for this object because RandomStringUtils.randomAlphanumeric( 5 )" ) );
    }

    public static Status status() {
        return new Status(
                request(),
                TestDataFactory.userPrincipal(),
                TestDataFactory.requestStatus() );
    }

    public static Map<UUID, Map<UUID, Set<Object>>> randomStringEntityData(
            int numberOfEntries,
            Set<UUID> propertyIds ) {
        Map<UUID, Map<UUID, Set<Object>>> data = new HashMap<>();
        for ( int i = 0; i < numberOfEntries; i++ ) {
            UUID entityId = UUID.randomUUID();
            Map<UUID, Set<Object>> entity = Maps.newHashMapWithExpectedSize( propertyIds.size() );
            for ( UUID propertyId : propertyIds ) {
                entity.put( propertyId, Set.of( randomAlphanumeric( 5 ) ) );
            }

            data.put( entityId, entity );
        }
        return data;
    }

    public static EntityKey entityKey() {
        return entityKey( UUID.randomUUID() );
    }

    public static EntityKey entityKey( UUID entitySetId ) {
        return new EntityKey( entitySetId, random( 10 ).replace( Character.MIN_VALUE, '0' ) );
    }

    public static EntityType entityTypesFromKeyAndTypes( PropertyType key, PropertyType... propertyTypes ) {
        final var propertyTags = new LinkedHashMap<UUID, LinkedHashSet<String>>();
        propertyTags.put( key.getId(), new LinkedHashSet<>() );
        propertyTags.get( key.getId() ).add( "PRIMARY KEY TAG" );
        return new EntityType( UUID.randomUUID(),
                fqn(),
                randomAlphanumeric( 5 ),
                Optional.empty(),
                ImmutableSet.of(),
                Stream.of( key ).map( PropertyType::getId )
                        .collect( Collectors.toCollection( LinkedHashSet::new ) ),
                Stream.concat( Stream.of( key ), Stream.of( propertyTypes ) ).map( PropertyType::getId )
                        .collect( Collectors.toCollection( LinkedHashSet::new ) ),
                propertyTags,
                Optional.empty(),
                Optional.empty(),
                Optional.empty() );
    }

    public static EntityTypePropertyMetadata entityTypePropertyMetadata() {
        return new EntityTypePropertyMetadata(
                randomAlphanumeric( 100 ), // title
                randomAlphanumeric( 100 ), // description
                Sets.newLinkedHashSet( Collections.singletonList( randomAlphanumeric( 5 ) ) ),
                r.nextBoolean()
        );
    }

    public static Map<UUID, Map<UUID, Set<Object>>> randomBinaryData(
            int numberOfEntries,
            UUID keyType,
            UUID binaryType ) {
        Map<UUID, Map<UUID, Set<Object>>> data = new HashMap<>();
        for ( int i = 0; i < numberOfEntries; i++ ) {
            data.put( UUID.randomUUID(), randomElement( keyType, binaryType ) );
        }

        return data;
    }

    public static Map<UUID, Set<Object>> randomElement( UUID keyType, UUID binaryType ) {
        return Map.of(
                keyType,
                Set.of( random( 5 ) ),
                binaryType,
                Set.of(
                        ImmutableMap.of( "content-type", "application/octet-stream", "data",
                                RandomUtils.nextBytes( 128 ) ),
                        ImmutableMap.of( "content-type", "application/octet-stream", "data",
                                RandomUtils.nextBytes( 128 ) ),
                        ImmutableMap.of( "content-type", "application/octet-stream", "data",
                                RandomUtils.nextBytes( 128 ) )
                ) );
    }

    public static MetadataUpdate metadataUpdate() {
        final var propertyTags = new LinkedHashMap<UUID, LinkedHashSet<String>>();
        propertyTags.put( UUID.randomUUID(), Sets.newLinkedHashSet( Set.of( "SOME PROPERTY TAG" ) ) );

        return new MetadataUpdate( Optional.of( randomAlphanumeric( 5 ) ),
                Optional.of( randomAlphanumeric( 5 ) ),
                Optional.empty(),
                Optional.of( new HashSet<>( Arrays.asList( randomAlphanumeric( 3 ),
                        randomAlphanumeric( 5 ) ) ) ),
                Optional.of( fqn() ),
                Optional.of( r.nextBoolean() ),
                Optional.empty(),
                Optional.of( randomAlphanumeric( 4 ) ),
                Optional.of( propertyTags ),
                Optional.of( UUID.randomUUID() ),
                Optional.of( new LinkedHashSet<>( Arrays.asList( 1, 2, 3, 4 ) ) ),
                Optional.empty() );
    }

    public static SearchDetails searchDetails() {
        return new SearchDetails( randomAlphanumeric( 10 ), UUID.randomUUID(), r.nextBoolean() );
    }

    public static SearchConstraints simpleSearchConstraints() {
        return SearchConstraints.simpleSearchConstraints( new UUID[] { UUID.randomUUID() },
                r.nextInt( 1000 ),
                r.nextInt( 1000 ),
                randomAlphanumeric( 10 ) );
    }

    public static PersistentSearch persistentSearch() {
        return new PersistentSearch( Optional.empty(),
                Optional.empty(),
                OffsetDateTime.now(),
                PersistentSearchNotificationType.ALPR_ALERT,
                simpleSearchConstraints(),
                ImmutableMap.of(),
                Optional.empty() );
    }

    public static CollectionTemplateType collectionTemplateType() {
        return new CollectionTemplateType(
                UUID.randomUUID(),
                randomAlphanumeric( 5 ),
                randomAlphanumeric( 5 ),
                Optional.of( randomAlphanumeric( 5 ) ),
                UUID.randomUUID()
        );
    }

    public static EntityTypeCollection entityTypeCollection() {
        return new EntityTypeCollection(
                UUID.randomUUID(),
                fqn(),
                randomAlphanumeric( 5 ),
                Optional.of( randomAlphanumeric( 5 ) ),
                ImmutableSet.of( fqn(), fqn(), fqn() ),
                Stream.of( collectionTemplateType(), collectionTemplateType(), collectionTemplateType() ).collect(
                        Collectors.toCollection( Sets::newLinkedHashSet ) )
        );
    }

    public static EntitySetCollection entitySetCollection() {
        return new EntitySetCollection(
                UUID.randomUUID(),
                randomAlphanumeric( 5 ),
                randomAlphanumeric( 5 ),
                Optional.of( randomAlphanumeric( 5 ) ),
                UUID.randomUUID(),
                ImmutableMap.of( UUID.randomUUID(),
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        UUID.randomUUID() ),
                ImmutableSet.of( randomAlphanumeric( 5 ),
                        randomAlphanumeric( 5 ),
                        randomAlphanumeric( 5 ) ),
                UUID.randomUUID()
        );
    }

    public static AppRole appRole() {
        return new AppRole(
                UUID.randomUUID(),
                randomAlphanumeric( 5 ),
                randomAlphanumeric( 5 ),
                randomAlphanumeric( 5 ),
                ImmutableMap.of( Permission.READ,
                        ImmutableMap.of( UUID.randomUUID(),
                                Optional.of( ImmutableSet
                                        .of( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() ) ) ),
                        Permission.WRITE,
                        ImmutableMap.of( UUID.randomUUID(), Optional.empty() ) ) );
    }

    public static App app() {
        return new App(
                UUID.randomUUID(),
                randomAlphanumeric( 5 ),
                randomAlphanumeric( 5 ),
                Optional.of( randomAlphanumeric( 5 ) ),
                randomAlphanumeric( 5 ),
                UUID.randomUUID(),
                ImmutableSet.of( appRole(), appRole(), appRole() ),
                ImmutableMap.of( randomAlphanumeric( 5 ),
                        randomAlphanumeric( 5 ),
                        randomAlphanumeric( 5 ),
                        false,
                        randomAlphanumeric( 5 ),
                        RandomUtils.nextInt( 0, 10000 ) ) );
    }

    public static AppConfigKey appConfigKey() {
        return new AppConfigKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    public static AppTypeSetting appConfigSetting() {
        return new AppTypeSetting( UUID.randomUUID(),
                UUID.randomUUID(),
                ImmutableMap
                        .of( UUID.randomUUID(), aclKey(), UUID.randomUUID(), aclKey(), UUID.randomUUID(), aclKey() ),
                app().getDefaultSettings() );
    }

    public static OrganizationExternalDatabaseColumn organizationExternalDatabaseColumn() {
        OrganizationExternalDatabaseTable table = organizationExternalDatabaseTable();
        return new OrganizationExternalDatabaseColumn(
                UUID.randomUUID(),
                randomAlphanumeric( 5 ),
                randomAlphanumeric( 5 ),
                Optional.of( randomAlphanumeric( 5 ) ),
                table.getId(),
                UUID.randomUUID(),
                PostgresDatatype.TEXT,
                r.nextBoolean(),
                r.nextInt( 1000 )
        );
    }

    public static OrganizationExternalDatabaseTable organizationExternalDatabaseTable() {
        return new OrganizationExternalDatabaseTable(
                UUID.randomUUID(),
                randomAlphanumeric( 5 ),
                randomAlphanumeric( 5 ),
                Optional.of( randomAlphanumeric( 5 ) ),
                UUID.randomUUID(),
                r.nextInt()
        );
    }

    public static PostgresAuthenticationRecord postgresAuthenticationRecord() {
        return new PostgresAuthenticationRecord(
                PostgresConnectionType.HOST,
                randomAlphanumeric( 5 ),
                randomAlphanumeric( 5 ),
                "0.0.0.0/0",
                randomAlphanumeric( 5 )
        );
    }

    public static EntitySetFlag entitySetFlag() {
        return entitySetFlags[ r.nextInt( entitySetFlags.length ) ];
    }

    public static SmsEntitySetInformation smsEntitySetInformation() {
        return new SmsEntitySetInformation(
                randomAlphanumeric( 12 ),
                UUID.randomUUID(),
                ImmutableSet.of( UUID.randomUUID() ),
                ImmutableSet.of( randomAlphanumeric( 5 ), randomAlphanumeric( 5 ) ),
                OffsetDateTime.now()
        );
    }

    public static Base64Media base64Media() {
        return new Base64Media(
                randomAlphabetic( 20 ),
                randomAlphabetic( 200 )
        );
    }

    public static MessageRequest messageRequest() {
        return new MessageRequest(
                UUID.randomUUID(),
                UUID.randomUUID(),
                randomAlphabetic( 20 ),
                ImmutableSet.of( randomAlphanumeric( 10 ) ),
                randomAlphanumeric( 15 ),
                base64Media(),
                OffsetDateTime.now()
        );
    }

    public static OrganizationDatabase organizationDatabase() {
        return new OrganizationDatabase( r.nextInt(), randomAlphanumeric( 10 ) );
    }

}