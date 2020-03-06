package com.openlattice.transporter.types

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component

@Component
class TransporterDatastore(configuration: TransporterConfiguration) {
    private val hds: HikariDataSource = HikariDataSource(HikariConfig(configuration.server))

    fun datastore(): HikariDataSource {
        return hds
    }
}