package com.openlattice.geocoding

import com.google.maps.model.AddressType
import com.google.maps.model.ComponentFilter
import com.google.maps.model.LatLng
import com.google.maps.model.LocationType
import java.util.*

/**
 * TODO: Add support for bounds and region.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class GeocodingRequest(
        val address: Optional<String>,
        val placeId: Optional<String>,
        val location: Optional<LatLng>,
        val components: Optional<Array<ComponentFilter>>,
        val resultType: Optional<Array<AddressType>>,
        val locationType: Optional<Array<LocationType>>
) {
    init {
        require(!(address.isPresent && location.isPresent && placeId.isPresent )) {
            "Must not have both address (forward geocoding) and location (reverse geocoding)."
        }

        require(placeId.isPresent || address.isPresent || location.isPresent || components.isPresent) {
            "One of place id, address, location, or components must be present."
        }
    }
}