

/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.requests;

import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.IMap;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.HzAuthzTest;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.requests.util.RequestUtil;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestsTests extends HzAuthzTest {
    protected static final RequestQueryService      aqs;
    protected static final HazelcastRequestsManager hzRequests;
    protected static final Lock                     lock      = new ReentrantLock();
    protected static final Status                   expected  = TestDataFactory.status();
    protected static final Status                   expected2 = new Status(
            expected.getRequest(),
            TestDataFactory.userPrincipal(),
            RequestStatus.SUBMITTED );
    protected static final Status                   expected3 = new Status(
            expected.getRequest(),
            TestDataFactory.userPrincipal(),
            RequestStatus.SUBMITTED );
    protected static final Status                   expected4 = new Status(
            TestDataFactory.aclKey(),
            expected.getRequest().getPermissions(),
            expected.getRequest().getReason(),
            expected.getPrincipal(),
            RequestStatus.SUBMITTED );
    protected static final Set<Status>              ss        = ImmutableSet.of( expected,
            expected2,
            expected3,
            expected4,
            TestDataFactory.status(),
            TestDataFactory.status(),
            TestDataFactory.status() );
    protected static final Set<Status>              submitted = ImmutableSet.of(
            expected2,
            expected3,
            expected4 );
    private static final   Logger                   logger    = LoggerFactory.getLogger( RequestsTests.class );

    static {
        IMap<AclKey, SecurableObjectType> objectTypes = hazelcastInstance
                .getMap( HazelcastMap.SECURABLE_OBJECT_TYPES.name() );
        for ( Status s : ss ) {
            boolean successful = false;
            while ( !successful ) {
                try {
                    Thread.sleep( 1000 );
                    objectTypes.set( s.getRequest().getAclKey(), SecurableObjectType.PropertyTypeInEntitySet );
                    successful = true;
                } catch ( RetryableHazelcastException e ) {
                    logger.info( "Unable to execute hazelcast operation waiting 1 s and retrying" );
                } catch ( InterruptedException e ) {
                    logger.info( "FML" );
                }

            }
        }
        aqs = new RequestQueryService( hds );
        hzRequests = new HazelcastRequestsManager( hazelcastInstance, aqs );
        Map<AceKey, Status> statusMap = RequestUtil.statusMap( ss );
        hzRequests.submitAll( statusMap );
    }

    @Test
    public void testSubmitAndGet() {
        Set<Status> expectedStatuses = ss.stream()
                .filter( status -> status.getPrincipal().equals( expected.getPrincipal() ) && status.getStatus()
                        .equals( expected.getStatus() ) )
                .collect( Collectors.toSet() );

        long c = expectedStatuses.size();

        Set<Status> statuses = hzRequests
                .getStatuses( expected.getPrincipal(), expected.getStatus() )
                .collect( Collectors.toSet() );
        Assert.assertEquals( c, statuses.size() );
        Assert.assertEquals( expectedStatuses, statuses );
    }

    @Test
    public void testSubmitAndGetByPrincipalAndStatus() {
        Set<Status> expectedStatuses = ss.stream()
                .filter( status -> status.getPrincipal().equals( expected.getPrincipal() ) && status.getStatus()
                        .equals( expected.getStatus() ) )
                .collect( Collectors.toSet() );
        long c = expectedStatuses.size();
        Set<Status> statuses = hzRequests
                .getStatuses( expected.getPrincipal(), expected.getStatus() )
                .collect(
                        Collectors.toSet() );
        Assert.assertEquals( c, statuses.size() );
        Assert.assertEquals( expectedStatuses, statuses );
    }

    @Test
    public void testSubmitAndGetByAclKey() {
        long c = ss.stream()
                .map( Status::getRequest )
                .map( Request::getAclKey )
                .filter( aclKey -> aclKey.equals( expected.getRequest().getAclKey() ) )
                .count();
        Assert.assertEquals( c, hzRequests.getStatusesForAllUser( expected.getRequest().getAclKey() ).count() );
    }

    @Test
    public void testSubmitAndGetByAclKeyAndStatus() {
        long c = ss.stream()
                .filter( status -> status.getRequest().getAclKey().equals( expected.getRequest().getAclKey() )
                        && status.getStatus()
                        .equals( RequestStatus.SUBMITTED ) )
                .count();
        Assert.assertEquals( c,
                hzRequests.getStatusesForAllUser( expected.getRequest().getAclKey(), RequestStatus.SUBMITTED )
                        .count() );
    }

    @Test
    public void testApproval() {
        Assert.assertTrue( submitted.stream().allMatch( s -> !hzAuthz
                .checkIfHasPermissions( new AclKey( s.getRequest().getAclKey() ),
                        ImmutableSet.of( s.getPrincipal() ),
                        s.getRequest().getPermissions() ) ) );
        ;
        hzRequests.submitAll( RequestUtil
                .statusMap( submitted.stream().map( RequestUtil::approve ).collect( Collectors.toSet() ) ) );

        Assert.assertTrue( submitted.stream().allMatch( s -> hzAuthz
                .checkIfHasPermissions( new AclKey( s.getRequest().getAclKey() ),
                        ImmutableSet.of( s.getPrincipal() ),
                        s.getRequest().getPermissions() ) ) );

    }
}
