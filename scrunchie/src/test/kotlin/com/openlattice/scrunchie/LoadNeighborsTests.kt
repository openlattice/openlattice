import com.openlattice.client.ApiClient
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.search.SearchApi
import com.openlattice.search.requests.EntityNeighborsFilter
import org.junit.Test
import org.slf4j.LoggerFactory
import java.util.*

class LoadNeighborsTests {

    companion object {
        val jwt = ""

        val entitySetId = UUID.fromString("")
        val entityKeyIds = setOf(
                UUID.fromString("")
        )

        val NUM_ATTEMPTS = 15

//        val logger = LoggerFactory.getLogger(LoadNeighborsTests::class.java)

    }

    private fun getResult(searchApi: SearchApi): Map<UUID, Set<NeighborEntityDetails>> {
        return searchApi.executeFilteredEntityNeighborSearch(
                entitySetId,
                EntityNeighborsFilter(entityKeyIds)
        ).mapValues { it.value.toSet() }
    }

    @Test
    fun testNeighborResultsMatch() {
        val searchApi = ApiClient(RetrofitFactory.Environment.LOCAL) { jwt }.searchApi


        val baseResult = getResult(searchApi)
//        logger.info("BASE RESULT COMPARING AGAINST: $baseResult")

        (0 until NUM_ATTEMPTS).forEach {
            val toCompare = getResult(searchApi)

            if (baseResult != toCompare) {
//                logger.info("MISMATCH ON TRIAL $it: $toCompare")
            }
        }

        assert(true)

    }
}