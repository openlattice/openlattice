package com.openlattice.hazelcast.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableList;
import com.openlattice.search.requests.*;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class SearchConstraintsStreamSerializer extends Serializer<SearchConstraints> {

    private static void writeUUID( Output output, UUID id ) {
        output.writeLong( id.getLeastSignificantBits() );
        output.writeLong( id.getMostSignificantBits() );
    }

    private static UUID readUUID( Input input ) {
        long lsb = input.readLong();
        long msb = input.readLong();
        return new UUID( msb, lsb );
    }

    @Override
    public void write( Kryo kryo, Output out, SearchConstraints object ) {
        serialize( out, object );
    }

    @Override
    public SearchConstraints read( Kryo kryo, Input in, Class<SearchConstraints> type ) {
        return deserialize( in );
    }

    public static void serialize( Output out, SearchConstraints object ) {

        out.writeInt( object.getEntitySetIds().length );
        for ( int i = 0; i < object.getEntitySetIds().length; i++ ) {
            writeUUID( out, object.getEntitySetIds()[ i ] );
        }

        out.writeInt( object.getStart() );
        out.writeInt( object.getMaxHits() );
        out.writeInt( object.getConstraintGroups().size() );

        for ( ConstraintGroup constraintGroup : object.getConstraintGroups() ) {
            out.writeInt( constraintGroup.getMinimumMatches() );
            out.writeInt( constraintGroup.getConstraints().size() );

            for ( Constraint constraint : constraintGroup.getConstraints() ) {
                out.writeString( constraint.getSearchType().name() );

                OptionalStreamSerializers.kryoSerialize( out, constraint.getSearchTerm(), Output::writeString );
                OptionalStreamSerializers.kryoSerialize( out, constraint.getFuzzy(), Output::writeBoolean );

                OptionalStreamSerializers.kryoSerialize( out, constraint.getSearches(), ( output, searches ) -> {
                    output.writeInt( searches.size() );
                    for ( SearchDetails searchDetails : searches ) {
                        output.writeString( searchDetails.getSearchTerm() );
                        writeUUID( output, searchDetails.getPropertyType() );
                        output.writeBoolean( searchDetails.getExactMatch() );
                    }
                } );

                OptionalStreamSerializers
                        .kryoSerialize( out,
                                constraint.getPropertyTypeId(),
                                SearchConstraintsStreamSerializer::writeUUID );
                OptionalStreamSerializers.kryoSerialize( out, constraint.getLatitude(), Output::writeDouble );
                OptionalStreamSerializers.kryoSerialize( out, constraint.getLongitude(), Output::writeDouble );
                OptionalStreamSerializers.kryoSerialize( out, constraint.getRadius(), Output::writeDouble );
                OptionalStreamSerializers
                        .kryoSerialize( out,
                                constraint.getDistanceUnit(),
                                ( output, distanceUnit ) -> output.writeString( distanceUnit.name() ) );

                OptionalStreamSerializers.kryoSerialize( out, constraint.getZones(), ( output, zones ) -> {
                    output.writeInt( zones.size() );
                    for ( List<List<Double>> zone : zones ) {
                        output.writeInt( zone.size() );
                        for ( List<Double> coordinate : zone ) {
                            output.writeDouble( coordinate.get( 0 ) );
                            output.writeDouble( coordinate.get( 1 ) );
                        }
                    }
                } );

                OptionalStreamSerializers.kryoSerialize( out, constraint.getStartDate(), ( output, date ) -> {
                    output.writeString( date.getOffset().getId() );
                    output.writeString( date.toLocalDateTime().toString() );
                } );

                OptionalStreamSerializers.kryoSerialize( out, constraint.getEndDate(), ( output, date ) -> {
                    output.writeString( date.getOffset().getId() );
                    output.writeString( date.toLocalDateTime().toString() );
                } );
            }
        }
    }

    public static SearchConstraints deserialize( Input in ) {
        int numEntitySetIds = in.readInt();
        UUID[] entitySetIds = new UUID[ numEntitySetIds ];
        for ( int i = 0; i < numEntitySetIds; i++ ) {
            entitySetIds[ i ] = readUUID( in );
        }

        int start = in.readInt();
        int maxHits = in.readInt();

        int numConstraintGroups = in.readInt();

        List<ConstraintGroup> constraintGroups = new ArrayList<>( numConstraintGroups );
        for ( int i = 0; i < numConstraintGroups; i++ ) {
            int minimumMatches = in.readInt();

            int numConstraints = in.readInt();
            List<Constraint> constraints = new ArrayList<>( numConstraints );

            for ( int j = 0; j < numConstraints; j++ ) {
                SearchType searchType = SearchType.valueOf( in.readString() );

                Optional<String> searchTerm = OptionalStreamSerializers.kryoDeserialize( in, Input::readString );
                Optional<Boolean> fuzzy = OptionalStreamSerializers.kryoDeserialize( in, Input::readBoolean );

                Optional<List<SearchDetails>> searches = OptionalStreamSerializers.kryoDeserialize( in, ( input ) -> {
                    int numSearches = input.readInt();
                    List<SearchDetails> searchDetailsList = new ArrayList<>( numSearches );
                    for ( int k = 0; k < numSearches; k++ ) {
                        String searchDetailTerm = input.readString();
                        UUID searchDetailProperty = readUUID( input );
                        boolean searchDetailExact = input.readBoolean();
                        searchDetailsList
                                .add( new SearchDetails( searchDetailTerm, searchDetailProperty, searchDetailExact ) );
                    }
                    return searchDetailsList;
                } );

                Optional<UUID> propertyTypeId = OptionalStreamSerializers
                        .kryoDeserialize( in, SearchConstraintsStreamSerializer::readUUID );
                Optional<Double> latitude = OptionalStreamSerializers.kryoDeserialize( in, Input::readDouble );
                Optional<Double> longitude = OptionalStreamSerializers.kryoDeserialize( in, Input::readDouble );
                Optional<Double> radius = OptionalStreamSerializers.kryoDeserialize( in, Input::readDouble );
                Optional<DistanceUnit> distanceUnit = OptionalStreamSerializers
                        .kryoDeserialize( in, ( input ) -> DistanceUnit.valueOf( input.readString() ) );

                Optional<List<List<List<Double>>>> zones = OptionalStreamSerializers.kryoDeserialize( in, ( input ) -> {
                    int numZones = input.readInt();
                    List<List<List<Double>>> zoneList = new ArrayList<>( numZones );
                    for ( int k = 0; k < numZones; k++ ) {
                        int numCoords = input.readInt();
                        List<List<Double>> coordList = new ArrayList<>( numCoords );

                        for ( int l = 0; l < numCoords; l++ ) {
                            double left = input.readDouble();
                            double right = input.readDouble();
                            coordList.add( ImmutableList.of( left, right ) );
                        }

                        zoneList.add( coordList );
                    }

                    return zoneList;
                } );

                Optional<OffsetDateTime> startDate = OptionalStreamSerializers
                        .kryoDeserialize( in, ( input ) -> {
                            ZoneOffset offset = ZoneOffset.of( input.readString() );
                            LocalDateTime ldt = LocalDateTime.parse( input.readString() );

                            return OffsetDateTime.of( ldt, offset );
                        } );

                Optional<OffsetDateTime> endDate = OptionalStreamSerializers
                        .kryoDeserialize( in, ( input ) -> {
                            ZoneOffset offset = ZoneOffset.of( input.readString() );
                            LocalDateTime ldt = LocalDateTime.parse( input.readString() );

                            return OffsetDateTime.of( ldt, offset );
                        } );

                constraints.add( new Constraint( searchType,
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
                        endDate ) );
            }

            constraintGroups.add( new ConstraintGroup( minimumMatches, constraints ) );

        }

        return new SearchConstraints( entitySetIds, start, maxHits, constraintGroups );
    }

}
