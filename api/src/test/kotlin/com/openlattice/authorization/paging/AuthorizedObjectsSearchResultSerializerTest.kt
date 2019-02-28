package com.openlattice.authorization.paging

import com.openlattice.authorization.AclKey
import com.openlattice.serializer.AbstractJacksonSerializationTest
import java.util.UUID

class AuthorizedObjectsSearchResultSerializerTest : AbstractJacksonSerializationTest<AuthorizedObjectsSearchResult>() {
    override fun getSampleData(): AuthorizedObjectsSearchResult {
        return AuthorizedObjectsSearchResult(
                "0",
                setOf(AclKey(UUID.randomUUID(), UUID.randomUUID())))
    }

    override fun getClazz(): Class<AuthorizedObjectsSearchResult> {
        return AuthorizedObjectsSearchResult::class.java
    }
}