package com.openlattice.shuttle.source

import java.io.InputStream

abstract class IntegrationOrigin : Sequence<InputStream> {
    abstract override fun iterator(): Iterator<InputStream>
}
