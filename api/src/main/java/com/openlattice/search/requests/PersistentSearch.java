package com.openlattice.search.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.search.PersistentSearchNotificationType;

import java.time.OffsetDateTime;
import java.util.*;

public class PersistentSearch {

    private final UUID                             id;
    private final OffsetDateTime                   lastRead;
    private final OffsetDateTime                   expiration;
    private final SearchConstraints                searchConstraints;
    private final PersistentSearchNotificationType type;
    private final Map<String, Object>              alertMetadata;
    private final Set<String>                      additionalEmailAddresses;

    @JsonCreator
    public PersistentSearch(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.LAST_READ ) Optional<OffsetDateTime> lastRead,
            @JsonProperty( SerializationConstants.EXPIRATION ) OffsetDateTime expiration,
            @JsonProperty( SerializationConstants.TYPE_FIELD ) PersistentSearchNotificationType type,
            @JsonProperty( SerializationConstants.CONSTRAINTS ) SearchConstraints searchConstraints,
            @JsonProperty( SerializationConstants.ALERT_METADATA ) Map<String, Object> alertMetadata,
            @JsonProperty( SerializationConstants.EMAILS_FIELD ) Optional<Set<String>> additionalEmailAddresses ) {
        this.id = id.orElse( UUID.randomUUID() );
        this.lastRead = lastRead.orElse( OffsetDateTime.now() );
        this.expiration = expiration;
        this.type = type;
        this.searchConstraints = searchConstraints;
        this.alertMetadata = alertMetadata;
        this.additionalEmailAddresses = additionalEmailAddresses.orElse( ImmutableSet.of() );

        Preconditions.checkNotNull( searchConstraints, "Search constraints must be present" );
        Preconditions.checkNotNull( expiration, "Expiration date must be present" );
        Preconditions.checkNotNull( type, "Type must be present." );
    }

    public PersistentSearch(
            UUID id,
            OffsetDateTime lastRead,
            OffsetDateTime expiration,
            PersistentSearchNotificationType type,
            SearchConstraints constraints,
            Map<String, Object> alertMetadata,
            Set<String> additionalEmailAddresses ) {
        this( Optional.of( id ), Optional.of( lastRead ), expiration, type, constraints, alertMetadata, Optional.of( additionalEmailAddresses ) );
    }

    @JsonProperty( SerializationConstants.ID_FIELD )
    public UUID getId() {
        return id;
    }

    @JsonProperty( SerializationConstants.LAST_READ )
    public OffsetDateTime getLastRead() {
        return lastRead;
    }

    @JsonProperty( SerializationConstants.EXPIRATION )
    public OffsetDateTime getExpiration() {
        return expiration;
    }

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public PersistentSearchNotificationType getType() {
        return type;
    }

    @JsonProperty( SerializationConstants.CONSTRAINTS )
    public SearchConstraints getSearchConstraints() {
        return searchConstraints;
    }

    @JsonProperty( SerializationConstants.ALERT_METADATA )
    public Map<String, Object> getAlertMetadata() {
        return alertMetadata;
    }

    @JsonProperty( SerializationConstants.EMAILS_FIELD )
    public Set<String> getAdditionalEmailAddresses() {
        return additionalEmailAddresses;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        PersistentSearch that = (PersistentSearch) o;
        return Objects.equals( id, that.id ) &&
                Objects.equals( lastRead, that.lastRead ) &&
                Objects.equals( expiration, that.expiration ) &&
                Objects.equals( searchConstraints, that.searchConstraints ) &&
                type == that.type &&
                Objects.equals( alertMetadata, that.alertMetadata ) &&
                Objects.equals( additionalEmailAddresses, that.additionalEmailAddresses );
    }

    @Override public int hashCode() {
        return Objects
                .hash( id, lastRead, expiration, searchConstraints, type, alertMetadata, additionalEmailAddresses );
    }
}
