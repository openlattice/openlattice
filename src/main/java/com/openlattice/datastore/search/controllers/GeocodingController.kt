package com.openlattice.datastore.search.controllers

import com.google.maps.GeoApiContext
import com.google.maps.PlaceAutocompleteRequest
import com.google.maps.PlacesApi
import com.google.maps.model.AutocompletePrediction
import com.google.maps.model.GeocodingResult
import com.openlattice.geocoding.AutocompleteRequest
import com.openlattice.geocoding.GeocodingApi
import com.openlattice.geocoding.GeocodingApi.Companion.AUTOCOMPLETE
import com.openlattice.geocoding.GeocodingApi.Companion.CONTROLLER
import com.openlattice.geocoding.GeocodingApi.Companion.GEOCODE
import com.openlattice.geocoding.GeocodingRequest
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(CONTROLLER)
class GeocodingController : GeocodingApi {
    @Inject
    private lateinit var geoApiContext: GeoApiContext

    @PostMapping(value = [AUTOCOMPLETE])
    override fun autocomplete(@RequestBody request: AutocompleteRequest): Array<out AutocompletePrediction> {
        val st = request.sessionToken.orElseGet(UUID::randomUUID)
        val autocompleteRequest = PlacesApi.placeAutocomplete(
                geoApiContext,
                request.input,
                PlaceAutocompleteRequest.SessionToken(st)
        )

        request.offset.ifPresent { autocompleteRequest.offset(it) }
        request.location.ifPresent { autocompleteRequest.location(it) }
        request.radius.ifPresent { autocompleteRequest.radius(it) }
        request.types.ifPresent { autocompleteRequest.types(it) }
        request.components.ifPresent { autocompleteRequest.components(*it) }


        return autocompleteRequest.await()
    }

    @PostMapping(value = [GEOCODE])
    override fun geocode(@RequestBody request: GeocodingRequest): Array<out GeocodingResult> {
        val req = if (request.address.isPresent) {
            com.google.maps.GeocodingApi.geocode(geoApiContext, request.address.get())
        } else {
            com.google.maps.GeocodingApi.reverseGeocode(geoApiContext, request.location.get())
        }

        request.placeId.ifPresent { req.place(it) }
        request.resultType.ifPresent { req.resultType(*it) }
        request.locationType.ifPresent { req.locationType(*it) }
        request.components.ifPresent { req.components(*it) }

        return req.await()
    }

}