package com.openlattice.ioc.providers

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
class LateInitProvidersPod {
    @Bean
    fun lateInitProvider() : LateInitProvider = OnUseLateInitProvider()
}