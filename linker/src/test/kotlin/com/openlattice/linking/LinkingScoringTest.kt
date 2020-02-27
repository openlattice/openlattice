package com.openlattice.linking;

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.linking.matching.getModelScore
import com.openlattice.linking.util.PersonMetric
import com.openlattice.rhizome.hazelcast.DelegatedStringSet
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport
import org.junit.Assert
import org.junit.Test
import org.nd4j.linalg.io.ClassPathResource
import java.io.File
import java.util.*

data class LinkingTestData (
        @JsonProperty("people") var people: Map<String, Map<UUID, Set<String>>>,
        @JsonProperty("fqnMap") val fqnMap: MutableMap<FullQualifiedName, UUID>,
        @JsonProperty("comparisons") val comparisons: List<LinkingTestComparison>
)

data class LinkingTestComparison (
        @JsonProperty("lhs") val lhs: String,
        @JsonProperty("rhs") val rhs: String,
        @JsonProperty("features") val features: List<Double>,
        @JsonProperty("score") val score: Double
)

open class LinkingScoringTest {

    @Test
    fun testFeatureExtraction() {

        // load model
        val simpleMlp = ClassPathResource("model_2019-01-30.h5").file.path
        val model = KerasModelImport.importKerasSequentialModelAndWeights( simpleMlp )

        // load input/output
        val configFile = ClassPathResource("scoringTest.yaml").file.path
        val configurations = ObjectMappers.getYamlMapper().readValue(File(configFile), LinkingTestData::class.java)

        // loop over comparisons
        for (comparison: LinkingTestComparison in configurations.comparisons) {

            val lhs = configurations.people?.get(comparison.lhs)
                    ?.mapValues{(_, v) -> DelegatedStringSet(v)}

            val rhs = configurations.people?.get(comparison.rhs)
                    ?.mapValues{(_, v) -> DelegatedStringSet(v)}

            // extract features
            val distance = PersonMetric.pDistance(lhs, rhs, configurations.fqnMap).toList()
            Assert.assertEquals(comparison.features, distance)

            // compute score
            val cleanFeatures = arrayOf(distance.toDoubleArray())
            val score = model.getModelScore(cleanFeatures ).get(0)
            Assert.assertEquals(comparison.score, score, 0.00001)

        }

    }

}

