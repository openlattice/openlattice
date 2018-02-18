

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

import static com.google.common.base.Preconditions.checkNotNull;

import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.processors.PermissionMerger;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.neuron.Neuron;
import com.openlattice.neuron.SignalType;
import com.openlattice.neuron.receptors.SignalQueueReceptor;
import com.openlattice.neuron.signals.Signal;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.datastore.util.Util;
import com.openlattice.authorization.AceValue;
import com.openlattice.authorization.AclKey;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.neuron.Neuron;
import com.openlattice.neuron.SignalType;
import com.openlattice.neuron.receptors.SignalQueueReceptor;
import com.openlattice.neuron.signals.Signal;
import com.openlattice.requests.RequestStatus;
import com.openlattice.requests.Status;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class HazelcastRequestsManager {

    private static final Logger                         logger              = LoggerFactory
            .getLogger( HazelcastRequestsManager.class );
    private static final Map<RequestStatus, SignalType> STATE_TO_SIGNAL_MAP = Maps.newEnumMap( RequestStatus.class );

    static {
        STATE_TO_SIGNAL_MAP.put( RequestStatus.APPROVED, SignalType.PERMISSION_REQUEST_APPROVED );
        STATE_TO_SIGNAL_MAP.put( RequestStatus.DECLINED, SignalType.PERMISSION_REQUEST_DECLINED );
        STATE_TO_SIGNAL_MAP.put( RequestStatus.SUBMITTED, SignalType.PERMISSION_REQUEST_SUBMITTED );
    }

    private final Neuron                            neuron;
    private final RequestQueryService               rqs;
    private final IMap<AceKey, Status>              requests;
    private final IMap<AceKey, AceValue>            aces;
    private final IMap<AclKey, SecurableObjectType> objectTypes;

    public HazelcastRequestsManager( HazelcastInstance hazelcastInstance, RequestQueryService rqs, Neuron neuron ) {

        this.requests = hazelcastInstance.getMap( HazelcastMap.REQUESTS.name() );
        this.aces = hazelcastInstance.getMap( HazelcastMap.PERMISSIONS.name() );
        this.objectTypes = hazelcastInstance.getMap( HazelcastMap.SECURABLE_OBJECT_TYPES.name() );
        this.rqs = checkNotNull( rqs );

        // TODO: it's not ideal to have to do "new SignalQueueReceptor( hazelcastInstance )"
        this.neuron = neuron;
        this.neuron.activateReceptor(
                EnumSet.of(
                        SignalType.PERMISSION_REQUEST_APPROVED,
                        SignalType.PERMISSION_REQUEST_DECLINED,
                        SignalType.PERMISSION_REQUEST_SUBMITTED
                ),
                new SignalQueueReceptor( hazelcastInstance )
        );
    }

    public void submitAll( Map<AceKey, Status> statusMap ) {
        statusMap
                .entrySet()
                .stream()
                .filter( e -> e.getValue().getStatus().equals( RequestStatus.APPROVED ) )
                .forEach( e -> aces.submitToKey(
                        e.getKey(),
                        new PermissionMerger(
                                e.getValue().getRequest().getPermissions(),
                                getNotNull( e.getKey().getAclKey() ) )
                ) );

        requests.putAll( statusMap );
        signalPermissionRequestStatusUpdates( statusMap );
    }

    public SecurableObjectType getNotNull( AclKey aclKey ) {
        return checkNotNull( Util.getSafely( objectTypes, aclKey ), "Securable Object Type isn't found" );
    }

    public Stream<Status> getStatuses( Principal principal ) {
        return getStatuses( rqs.getRequestKeys( principal ) );
    }

    public Stream<Status> getStatuses( Principal principal, RequestStatus requestStatus ) {
        return getStatuses( rqs.getRequestKeys( principal, requestStatus ) );
    }

    public Stream<Status> getStatusesForAllUser( List<UUID> aclKey ) {
        return getStatuses( rqs.getRequestKeys( aclKey ) );
    }

    public Stream<Status> getStatusesForAllUser( List<UUID> aclKey, RequestStatus requestStatus ) {
        return getStatuses( rqs.getRequestKeys( aclKey, requestStatus ) );
    }

    public Stream<Status> getStatuses( Stream<AceKey> requestKeys ) {
        return requestKeys.map( Util.getSafeMapper( requests ) );
    }

    private void signalPermissionRequestStatusUpdates( Map<AceKey, Status> aceKeyRequestStateMap ) {

        /*
         * TODO: figure out a better approach to prepping this signal. since this is a very specific use case,
         * perhaps it makes sense to have a specific Signal or Receptor that handles this logic.
         */

        Map<RequestStatus, Set<AceKey>> stateToAceKeysMap = Maps.newEnumMap( RequestStatus.class );
        stateToAceKeysMap.put( RequestStatus.APPROVED, Sets.newHashSet() );
        stateToAceKeysMap.put( RequestStatus.DECLINED, Sets.newHashSet() );
        stateToAceKeysMap.put( RequestStatus.SUBMITTED, Sets.newHashSet() );

        aceKeyRequestStateMap.forEach( ( aceKey, requestStatus ) -> {

            AclKey newAclKey = aceKey.getAclKey();
            if ( newAclKey.size() > 1 ) {
                // we need a new ArrayList, otherwise we get "java.io.NotSerializableException: java.util.ArrayList$SubList"
                newAclKey = new AclKey( Lists.newArrayList( newAclKey.subList( 0, newAclKey.size() - 1 ) ) );
            }

            stateToAceKeysMap
                    .get( requestStatus.getStatus() )
                    .add( new AceKey( newAclKey, requestStatus.getPrincipal() ) );
        } );

        stateToAceKeysMap.forEach( ( state, aceKeys ) -> {
            aceKeys.forEach( aceKey -> {
                this.neuron.transmit( new Signal(
                        STATE_TO_SIGNAL_MAP.get( state ),
                        aceKey.getAclKey(),
                        aceKey.getPrincipal()
                ) );
            } );
        } );
    }
}
