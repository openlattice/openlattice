package com.openlattice.geocoding

import com.google.maps.model.AutocompletePrediction
import com.google.maps.model.GeocodingResult
import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.Path

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
        const val PLACE = "/place"
        const val GEOCODE = "/geocode"

        const val ID = "id"
        const val ID_PATH = "/{$ID}"
    }

    @POST(BASE + AUTOCOMPLETE)
    fun autocomplete(@Body request: AutocompleteRequest): Array<out AutocompletePrediction>


    @GET(BASE + PLACE + ID_PATH)
    fun geocodePlace(@Path(ID) placeId: String): Array<out GeocodingResult>

    @POST(BASE + GEOCODE)
    fun geocode(@Body request: GeocodingRequest): Array<out GeocodingResult>
}