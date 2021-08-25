package com.openlattice.rehearsal.load;

import com.openlattice.rehearsal.SetupTestData;
import kotlin.jvm.JvmStatic;
import org.junit.*;
import org.slf4j.LoggerFactory;

class LoadedLinkingTest : SetupTestData() {
    companion object {
        private val logger = LoggerFactory.getLogger( LoadedLinkingTest::class.java)
    }

    @Test @Ignore
    fun bigIntegration() {
        importDataSetFromAtlas(
                "socratesAtlas.yaml", "select * from public.socrates limit 1000;",
                "socratesAtlasConfiguration.yaml", "example_data" )
    }

}
