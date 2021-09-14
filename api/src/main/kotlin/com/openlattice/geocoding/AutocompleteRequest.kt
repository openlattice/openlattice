package com.openlattice.geocoding

import com.google.maps.model.ComponentFilter
import com.google.maps.model.LatLng
import com.google.maps.model.PlaceAutocompleteType
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class AutocompleteRequest(
        val input: String,
        val sessionToken: Optional<UUID>,
        val offset: Optional<Int>,
        val location: Optional<LatLng>,
        val radius: Optional<Int>,
        val types: Optional<PlaceAutocompleteType>,
        private val componentFilters: Optional<Array<ComponentFilterWrapper>>
) {
    val components: Optional<Array<ComponentFilter>> = componentFilters.map { cfws ->
        cfws.map { cfw -> ComponentFilter(cfw.component, cfw.value) }.toTypedArray()
    }
}