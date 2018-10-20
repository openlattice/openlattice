package com.openlattice.search.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class Constraint {

    private final SearchType searchType;

    // standard data search
    private final Optional<String>  searchTerm;
    private final Optional<Boolean> fuzzy;

    // advanced search
    private final Optional<List<SearchDetails>> searches;

    // geo searches (distance + polygon)
    private final Optional<UUID> propertyTypeId;

    // geo distance search
    private final Optional<Double>       latitude;
    private final Optional<Double>       longitude;
    private final Optional<Double>       radius;
    private final Optional<DistanceUnit> distanceUnit;

    // geo polygon search
    private final Optional<List<List<List<Double>>>> zones;

    @JsonCreator
    public Constraint(
            @JsonProperty( SerializationConstants.TYPE_FIELD ) Optional<SearchType> searchType,
            @JsonProperty( SerializationConstants.SEARCH_TERM ) Optional<String> searchTerm,
            @JsonProperty( SerializationConstants.FUZZY ) Optional<Boolean> fuzzy,
            @JsonProperty( SerializationConstants.SEARCH_FIELDS ) Optional<List<SearchDetails>> searches,
            @JsonProperty( SerializationConstants.PROPERTY_TYPE_ID ) Optional<UUID> propertyTypeId,
            @JsonProperty( SerializationConstants.LATITUDE ) Optional<Double> latitude,
            @JsonProperty( SerializationConstants.LONGITUDE ) Optional<Double> longitude,
            @JsonProperty( SerializationConstants.RADIUS ) Optional<Double> radius,
            @JsonProperty( SerializationConstants.UNIT ) Optional<DistanceUnit> distanceUnit,
            @JsonProperty( SerializationConstants.ZONES ) Optional<List<List<List<Double>>>> zones

    ) {

        this.searchType = searchType.orElse( SearchType.simple );

        this.searchTerm = searchTerm;
        this.fuzzy = this.searchType.equals( SearchType.simple ) ? Optional.of( fuzzy.orElse( false ) ) : fuzzy;

        this.searches = searches;

        this.propertyTypeId = propertyTypeId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.radius = radius;
        this.distanceUnit = distanceUnit;

        this.zones = zones;

        // validation
        switch ( this.searchType ) {
            case advanced:
                Preconditions.checkArgument( this.searches.isPresent(),
                        "Field searches must be present for searches of type advanced" );
                break;

            case geoDistance:
                Preconditions.checkArgument( this.propertyTypeId.isPresent(),
                        "Field propertyTypeId must be present for searches of type geoDistance" );
                Preconditions.checkArgument( this.latitude.isPresent(),
                        "Field latitude must be present for searches of type geoDistance" );
                Preconditions.checkArgument( this.longitude.isPresent(),
                        "Field longitude must be present for searches of type geoDistance" );
                Preconditions.checkArgument( this.radius.isPresent(),
                        "Field radius must be present for searches of type geoDistance" );
                Preconditions.checkArgument( this.distanceUnit.isPresent(),
                        "Field distanceUnit must be present for searches of type geoDistance" );
                break;

            case geoPolygon:
                Preconditions.checkArgument( this.propertyTypeId.isPresent(),
                        "Field propertyTypeId must be present for searches of type geoPolygon" );
                Preconditions.checkArgument( this.zones.isPresent() && this.zones.get().size() > 0,
                        "Field zones must be present and non-empty for searches of type geoPolygon" );
                break;

            case simple:
                Preconditions.checkArgument( this.searchTerm.isPresent(),
                        "Field searchTerm must be present for searches of type simple" );
                Preconditions.checkArgument( this.fuzzy.isPresent(),
                        "Field fuzzy must be present for searches of type simple" );
                break;

        }
    }

    public Constraint(
            SearchType searchType,
            Optional<String> searchTerm,
            Optional<Boolean> fuzzy,
            Optional<List<SearchDetails>> searches,
            Optional<UUID> propertyTypeId,
            Optional<Double> latitude,
            Optional<Double> longitude,
            Optional<Double> radius,
            Optional<DistanceUnit> distanceUnit,
            Optional<List<List<List<Double>>>> zones ) {
        this( Optional.of( searchType ),
                searchTerm,
                fuzzy,
                searches,
                propertyTypeId,
                latitude,
                longitude,
                radius,
                distanceUnit,
                zones );
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

    @JsonProperty( SerializationConstants.ZONES )
    public Optional<List<List<List<Double>>>> getZones() {
        return zones;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        Constraint that = (Constraint) o;
        return searchType == that.searchType &&
                Objects.equals( searchTerm, that.searchTerm ) &&
                Objects.equals( fuzzy, that.fuzzy ) &&
                Objects.equals( searches, that.searches ) &&
                Objects.equals( propertyTypeId, that.propertyTypeId ) &&
                Objects.equals( latitude, that.latitude ) &&
                Objects.equals( longitude, that.longitude ) &&
                Objects.equals( radius, that.radius ) &&
                Objects.equals( distanceUnit, that.distanceUnit ) &&
                Objects.equals( zones, that.zones );
    }

    @Override public int hashCode() {

        return Objects.hash( searchType,
                searchTerm,
                fuzzy,
                searches,
                propertyTypeId,
                latitude,
                longitude,
                radius,
                distanceUnit,
                zones );
    }
}
