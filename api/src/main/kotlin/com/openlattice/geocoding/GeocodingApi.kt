package com.openlattice.geocoding

import com.google.maps.model.AutocompletePrediction
import com.google.maps.model.GeocodingResult
import com.google.maps.model.PlacesSearchResult
import retrofit2.http.Body
import retrofit2.http.Query
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface GeocodingApi {
    companion object {
        const val INPUT = "input"
        const val SESSION_TOKEN = "sessionToken"
        const val OFFSET = "offset"
        const val ORIGIN = "origin"
        const val RADIUS = "radius"
        const val TYPES = "types"
        const val COMPONENTS = "components"
    }

    fun autocomplete(@Body request: AutocompleteRequest): Array<out AutocompletePrediction>
    fun geocode(@Body request:GeocodingRequest): Array<out GeocodingResult>
}