package com.openlattice.geocoding

import com.google.maps.model.AutocompletePrediction
import com.google.maps.model.GeocodingResult
import retrofit2.http.Body
import retrofit2.http.POST

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface GeocodingApi {
    companion object {
        const val SERVICE = "/datastore"
        const val CONTROLLER = "/geocoding"
        const val BASE = SERVICE + CONTROLLER

        const val AUTOCOMPLETE = "/autocomplete"
        const val GEOCODE = "/geocode"
    }

    @POST(BASE + AUTOCOMPLETE)
    fun autocomplete(@Body request: AutocompleteRequest): Array<out AutocompletePrediction>

    @POST(BASE + GEOCODE)
    fun geocode(@Body request: GeocodingRequest): Array<out GeocodingResult>
}