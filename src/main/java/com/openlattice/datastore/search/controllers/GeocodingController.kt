package com.openlattice.datastore.search.controllers

import com.google.maps.GeoApiContext
import com.google.maps.PlaceAutocompleteRequest
import com.google.maps.PlacesApi
import com.google.maps.model.AutocompletePrediction
import com.google.maps.model.GeocodingResult
import com.openlattice.geocoding.AutocompleteRequest
import com.openlattice.geocoding.GeocodingApi
import com.openlattice.geocoding.GeocodingRequest
import java.util.*
import javax.inject.Inject


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class GeocodingController : GeocodingApi {
    @Inject
    private lateinit var geoApiContext: GeoApiContext

    override fun autocomplete(request: AutocompleteRequest): Array<out AutocompletePrediction> {
        val st = request.sessionToken.orElseGet(UUID::randomUUID)
        val autocompleteRequest = PlacesApi.placeAutocomplete(
                geoApiContext,
                request.input,
                PlaceAutocompleteRequest.SessionToken(st)
        )

        request.offset.ifPresent( autocompleteRequest::offset )
        request.location.ifPresent(autocompleteRequest::location )
        request.radius.ifPresent(autocompleteRequest::radius)
        request.types.ifPresent(autocompleteRequest::types)
        request.components.ifPresent(autocompleteRequest::components)


        return autocompleteRequest.await()
    }

    override fun geocode(request: GeocodingRequest): Array<out GeocodingResult> {
        val req = if( request.address.isPresent ) {
             com.google.maps.GeocodingApi.geocode(geoApiContext, request.address.get())
        } else {
            com.google.maps.GeocodingApi.reverseGeocode(geoApiContext, request.location.get() )
        }

        request.placeId.ifPresent(req::place)
        request.resultType.ifPresent(req::resultType)
        request.locationType.ifPresent(req::locationType)
        request.components.ifPresent(req::components)

        return req.await()
    }

}