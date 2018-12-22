package com.openlattice.search.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.search.SearchApi;

import java.time.OffsetDateTime;
import java.util.*;

public class SearchConstraints {

    // always required
    private final UUID[]                entitySetIds;
    private final int                   start;
    private final int                   maxHits;
    private final List<ConstraintGroup> constraintGroups;

    @JsonCreator
    public SearchConstraints(
            @JsonProperty( SerializationConstants.ENTITY_SET_IDS ) UUID[] entitySetIds,
            @JsonProperty( SerializationConstants.START ) int start,
            @JsonProperty( SerializationConstants.MAX_HITS ) int maxHits,
            @JsonProperty( SerializationConstants.CONSTRAINTS ) List<ConstraintGroup> constraintGroups

    ) {

        this.entitySetIds = Arrays.copyOf( entitySetIds, entitySetIds.length );
        this.start = start;
        this.maxHits = Math.min( maxHits, SearchApi.MAX_SEARCH_RESULTS );
        this.constraintGroups = constraintGroups;

        Preconditions
                .checkArgument( constraintGroups.size() > 0, "SearchConstraints constraintGroups cannot be empty" );

    }

    public SearchConstraints(
            UUID[] entitySetIds,
            int start,
            int maxHits,
            SearchType searchType,
            Optional<String> searchTerm,
            Optional<Boolean> fuzzy,
            Optional<List<SearchDetails>> searches,
            Optional<UUID> propertyTypeId,
            Optional<Double> latitude,
            Optional<Double> longitude,
            Optional<Double> radius,
            Optional<DistanceUnit> distanceUnit,
            Optional<List<List<List<Double>>>> zones,
            Optional<OffsetDateTime> startDate,
            Optional<OffsetDateTime> endDate ) {
        this( entitySetIds,
                start,
                maxHits,
                ImmutableList.of( new ConstraintGroup( ImmutableList.of( new Constraint(
                        Optional.of( searchType ),
                        searchTerm,
                        fuzzy,
                        searches,
                        propertyTypeId,
                        latitude,
                        longitude,
                        radius,
                        distanceUnit,
                        zones,
                        startDate,
                        endDate
                ) ) ) ) );
    }

    public static SearchConstraints simpleSearchConstraints(
            UUID[] entitySetIds,
            int start,
            int maxHits,
            String searchTerm ) {
        return simpleSearchConstraints( entitySetIds, start, maxHits, searchTerm, false );
    }

    public static SearchConstraints simpleSearchConstraints(
            UUID[] entitySetIds,
            int start,
            int maxHits,
            String searchTerm,
            boolean fuzzy ) {
        return new SearchConstraints( entitySetIds,
                start,
                maxHits,
                ImmutableList.of( new ConstraintGroup( ImmutableList.of( new Constraint(
                        Optional.of( SearchType.simple ),
                        Optional.of( searchTerm ),
                        Optional.of( fuzzy ),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty() ) ) ) ) );
    }

    public static SearchConstraints advancedSearchConstraints(
            UUID[] entitySetIds,
            int start,
            int maxHits,
            List<SearchDetails> searches ) {
        return new SearchConstraints( entitySetIds,
                start,
                maxHits,
                ImmutableList.of( new ConstraintGroup( ImmutableList.of( new Constraint(
                        Optional.of( SearchType.advanced ),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of( searches ),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty() ) ) ) ) );
    }

    public static SearchConstraints geoDistanceSearchConstraints(
            UUID[] entitySetIds,
            int start,
            int maxHits,
            UUID propertyTypeId,
            double latitude,
            double longitude,
            double radius,
            DistanceUnit distanceUnit ) {
        return new SearchConstraints( entitySetIds,
                start,
                maxHits,
                ImmutableList.of( new ConstraintGroup( ImmutableList.of( new Constraint(
                        Optional.of( SearchType.geoDistance ),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of( propertyTypeId ),
                        Optional.of( latitude ),
                        Optional.of( longitude ),
                        Optional.of( radius ),
                        Optional.of( distanceUnit ),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty() ) ) ) ) );
    }

    public static SearchConstraints geoPolygonSearchConstraints(
            UUID[] entitySetIds,
            int start,
            int maxHits,
            UUID propertyTypeId,
            List<List<List<Double>>> zones ) {
        return new SearchConstraints( entitySetIds,
                start,
                maxHits,
                ImmutableList.of( new ConstraintGroup( ImmutableList.of( new Constraint(
                        Optional.of( SearchType.geoDistance ),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of( propertyTypeId ),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of( zones ),
                        Optional.empty(),
                        Optional.empty() ) ) ) ) );
    }

    public static SearchConstraints writeDateTimeFilterConstraints(
            UUID[] entitySetIds,
            int start,
            int maxHits,
            Optional<OffsetDateTime> startDate,
            Optional<OffsetDateTime> endDate
    ) {
        return new SearchConstraints( entitySetIds,
                start,
                maxHits,
                ImmutableList.of( new ConstraintGroup( ImmutableList.of( new Constraint(
                        Optional.of( SearchType.writeDateTimeFilter ),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        startDate,
                        endDate
                ) ) ) ) );
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_IDS )
    public UUID[] getEntitySetIds() {
        return Arrays.copyOf( entitySetIds, entitySetIds.length );
    }

    @JsonProperty( SerializationConstants.START )
    public int getStart() {
        return start;
    }

    @JsonProperty( SerializationConstants.MAX_HITS )
    public int getMaxHits() {
        return maxHits;
    }

    @JsonProperty( SerializationConstants.CONSTRAINTS )
    public List<ConstraintGroup> getConstraintGroups() {
        return constraintGroups;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        SearchConstraints that = (SearchConstraints) o;
        return start == that.start &&
                maxHits == that.maxHits &&
                Arrays.equals( entitySetIds, that.entitySetIds ) &&
                Objects.equals( constraintGroups, that.constraintGroups );
    }

    @Override public int hashCode() {

        int result = Objects.hash( start, maxHits, constraintGroups );
        result = 31 * result + Arrays.hashCode( entitySetIds );
        return result;
    }
}
