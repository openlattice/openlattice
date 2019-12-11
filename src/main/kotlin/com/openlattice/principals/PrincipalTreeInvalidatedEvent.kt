package com.openlattice.principals

import com.geekbeast.rhizome.async.AlwaysPublishToHazelcast

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PrincipalTreeInvalidatedEvent(val principalId: String) : AlwaysPublishToHazelcast