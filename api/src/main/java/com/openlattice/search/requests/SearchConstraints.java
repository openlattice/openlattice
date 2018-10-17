package com.openlattice.search.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.search.SearchApi;

import java.util.*;

public class SearchConstraints {

    // always required
    private final UUID[]     entitySetIds;
    private final int        start;
    private final int        maxHits;
    private final SearchType searchType;

    // standard data search
    private final Optional<String>  searchTerm;
    private final Optional<Boolean> fuzzy;

    // advanced search
    private final Optional<List<SearchDetails>> searches;

    // geo distance search
    private final Optional<UUID>         propertyTypeId;
    private final Optional<Double>       latitude;
    private final Optional<Double>       longitude;
    private final Optional<Double>       radius;
    private final Optional<DistanceUnit> distanceUnit;

    @JsonCreator
    public SearchConstraints(
            @JsonProperty( SerializationConstants.ENTITY_SET_IDS ) UUID[] entitySetIds,
            @JsonProperty( SerializationConstants.START ) int start,
            @JsonProperty( SerializationConstants.MAX_HITS ) int maxHits,
            @JsonProperty( SerializationConstants.TYPE_FIELD ) Optional<SearchType> searchType,
            @JsonProperty( SerializationConstants.SEARCH_TERM ) Optional<String> searchTerm,
            @JsonProperty( SerializationConstants.FUZZY ) Optional<Boolean> fuzzy,
            @JsonProperty( SerializationConstants.SEARCH_FIELDS ) Optional<List<SearchDetails>> searches,
            @JsonProperty( SerializationConstants.PROPERTY_TYPE_ID ) Optional<UUID> propertyTypeId,
            @JsonProperty( SerializationConstants.LATITUDE ) Optional<Double> latitude,
            @JsonProperty( SerializationConstants.LONGITUDE ) Optional<Double> longitude,
            @JsonProperty( SerializationConstants.RADIUS ) Optional<Double> radius,
            @JsonProperty( SerializationConstants.UNIT ) Optional<DistanceUnit> distanceUnit

    ) {

        // initialization

        this.entitySetIds = Arrays.copyOf( entitySetIds, entitySetIds.length );
        this.start = start;
        this.maxHits = Math.min( maxHits, SearchApi.MAX_SEARCH_RESULTS );
        this.searchType = searchType.orElse( SearchType.simple );

        this.searchTerm = searchTerm;
        this.fuzzy = this.searchType.equals( SearchType.simple ) ? Optional.of( fuzzy.orElse( false ) ) : fuzzy;

        this.searches = searches;

        this.propertyTypeId = propertyTypeId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.radius = radius;
        this.distanceUnit = distanceUnit;

        // validation
        switch ( this.searchType ) {
            case advanced:
                Preconditions.checkArgument( this.searches.isPresent(),
                        "Field searches must be present for searches of type advanced" );
                break;

            case geoDistance:
                Preconditions.checkArgument( this.propertyTypeId.isPresent(),
                        "Field propertyTypeId must be present for searches of type geo_distance" );
                Preconditions.checkArgument( this.latitude.isPresent(),
                        "Field latitude must be present for searches of type geo_distance" );
                Preconditions.checkArgument( this.longitude.isPresent(),
                        "Field longitude must be present for searches of type geo_distance" );
                Preconditions.checkArgument( this.radius.isPresent(),
                        "Field radius must be present for searches of type geo_distance" );
                Preconditions.checkArgument( this.distanceUnit.isPresent(),
                        "Field distanceUnit must be present for searches of type geo_distance" );
                break;

            case simple:
                Preconditions.checkArgument( this.searchTerm.isPresent(),
                        "Field searchTerm must be present for searches of type simple" );
                Preconditions.checkArgument( this.fuzzy.isPresent(),
                        "Field fuzzy must be present for searches of type simple" );
                break;

        }
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
            Optional<DistanceUnit> distanceUnit ) {
        this( entitySetIds,
                start,
                maxHits,
                Optional.of( searchType ),
                searchTerm,
                fuzzy,
                searches,
                propertyTypeId,
                latitude,
                longitude,
                radius,
                distanceUnit );
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
                Optional.of( SearchType.simple ),
                Optional.of( searchTerm ),
                Optional.of( fuzzy ),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty() );
    }

    public static SearchConstraints advancedSearchConstraints(
            UUID[] entitySetIds,
            int start,
            int maxHits,
            List<SearchDetails> searches ) {
        return new SearchConstraints( entitySetIds,
                start,
                maxHits,
                Optional.of( SearchType.advanced ),
                Optional.empty(),
                Optional.empty(),
                Optional.of( searches ),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty() );
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
                Optional.of( SearchType.geoDistance ),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of( propertyTypeId ),
                Optional.of( latitude ),
                Optional.of( longitude ),
                Optional.of( radius ),
                Optional.of( distanceUnit ) );
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

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public SearchType getSearchType() {
        return searchType;
    }

    @JsonProperty( SerializationConstants.SEARCH_TERM )
    public Optional<String> getSearchTerm() {
        return searchTerm;
    }

    @JsonProperty( SerializationConstants.FUZZY )
    public Optional<Boolean> getFuzzy() {
        return fuzzy;
    }

    @JsonProperty( SerializationConstants.SEARCH_FIELDS )
    public Optional<List<SearchDetails>> getSearches() {
        return searches;
    }

    @JsonProperty( SerializationConstants.PROPERTY_TYPE_ID )
    public Optional<UUID> getPropertyTypeId() {
        return propertyTypeId;
    }

    @JsonProperty( SerializationConstants.LATITUDE )
    public Optional<Double> getLatitude() {
        return latitude;
    }

    @JsonProperty( SerializationConstants.LONGITUDE )
    public Optional<Double> getLongitude() {
        return longitude;
    }

    @JsonProperty( SerializationConstants.RADIUS )
    public Optional<Double> getRadius() {
        return radius;
    }

    @JsonProperty( SerializationConstants.UNIT )
    public Optional<DistanceUnit> getDistanceUnit() {
        return distanceUnit;
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
                searchType == that.searchType &&
                Objects.equals( searchTerm, that.searchTerm ) &&
                Objects.equals( fuzzy, that.fuzzy ) &&
                Objects.equals( searches, that.searches ) &&
                Objects.equals( propertyTypeId, that.propertyTypeId ) &&
                Objects.equals( latitude, that.latitude ) &&
                Objects.equals( longitude, that.longitude ) &&
                Objects.equals( radius, that.radius ) &&
                Objects.equals( distanceUnit, that.distanceUnit );
    }

    @Override public int hashCode() {

        int result = Objects.hash( start,
                maxHits,
                searchType,
                searchTerm,
                fuzzy,
                searches,
                propertyTypeId,
                latitude,
                longitude,
                radius,
                distanceUnit );
        result = 31 * result + Arrays.hashCode( entitySetIds );
        return result;
    }
}
