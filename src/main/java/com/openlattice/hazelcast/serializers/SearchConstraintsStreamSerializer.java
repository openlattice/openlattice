package com.openlattice.hazelcast.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableList;
import com.openlattice.search.requests.DistanceUnit;
import com.openlattice.search.requests.SearchConstraints;
import com.openlattice.search.requests.SearchDetails;
import com.openlattice.search.requests.SearchType;

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
        out.writeString( object.getSearchType().name() );

        OptionalStreamSerializers.kryoSerialize( out, object.getSearchTerm(), Output::writeString );
        OptionalStreamSerializers.kryoSerialize( out, object.getFuzzy(), Output::writeBoolean );

        OptionalStreamSerializers.kryoSerialize( out, object.getSearches(), ( output, searches ) -> {
            output.writeInt( searches.size() );
            for ( SearchDetails searchDetails : searches ) {
                output.writeString( searchDetails.getSearchTerm() );
                writeUUID( output, searchDetails.getPropertyType() );
                output.writeBoolean( searchDetails.getExactMatch() );
            }
        } );

        OptionalStreamSerializers
                .kryoSerialize( out, object.getPropertyTypeId(), SearchConstraintsStreamSerializer::writeUUID );
        OptionalStreamSerializers.kryoSerialize( out, object.getLatitude(), Output::writeDouble );
        OptionalStreamSerializers.kryoSerialize( out, object.getLongitude(), Output::writeDouble );
        OptionalStreamSerializers.kryoSerialize( out, object.getRadius(), Output::writeDouble );
        OptionalStreamSerializers
                .kryoSerialize( out,
                        object.getDistanceUnit(),
                        ( output, distanceUnit ) -> output.writeString( distanceUnit.name() ) );

        OptionalStreamSerializers.kryoSerialize( out, object.getZones(), ( output, zones ) -> {
            output.writeInt( zones.size() );
            for ( List<List<Double>> zone : zones ) {
                output.writeInt( zone.size() );
                for ( List<Double> coordinate : zone ) {
                    output.writeDouble( coordinate.get( 0 ) );
                    output.writeDouble( coordinate.get( 1 ) );
                }
            }
        } );
    }

    public static SearchConstraints deserialize( Input in ) {
        int numEntitySetIds = in.readInt();
        UUID[] entitySetIds = new UUID[ numEntitySetIds ];
        for ( int i = 0; i < numEntitySetIds; i++ ) {
            entitySetIds[ i ] = readUUID( in );
        }

        int start = in.readInt();
        int maxHits = in.readInt();
        SearchType searchType = SearchType.valueOf( in.readString() );

        Optional<String> searchTerm = OptionalStreamSerializers.kryoDeserialize( in, Input::readString );
        Optional<Boolean> fuzzy = OptionalStreamSerializers.kryoDeserialize( in, Input::readBoolean );

        Optional<List<SearchDetails>> searches = OptionalStreamSerializers.kryoDeserialize( in, ( input ) -> {
            int numSearches = input.readInt();
            List<SearchDetails> searchDetailsList = new ArrayList<>( numSearches );
            for ( int i = 0; i < numSearches; i++ ) {
                String searchDetailTerm = input.readString();
                UUID searchDetailProperty = readUUID( input );
                boolean searchDetailExact = input.readBoolean();
                searchDetailsList.add( new SearchDetails( searchDetailTerm, searchDetailProperty, searchDetailExact ) );
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
            for ( int i = 0; i < numZones; i++ ) {
                int numCoords = input.readInt();
                List<List<Double>> coordList = new ArrayList<>( numCoords );

                for ( int j = 0; j < numCoords; j++ ) {
                    double left = input.readDouble();
                    double right = input.readDouble();
                    coordList.add( ImmutableList.of( left, right ) );
                }

                zoneList.add( coordList );
            }

            return zoneList;
        } );

        return new SearchConstraints( entitySetIds,
                start,
                maxHits,
                searchType,
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
