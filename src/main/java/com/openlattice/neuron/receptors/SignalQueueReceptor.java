

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

package com.openlattice.neuron.receptors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openlattice.hazelcast.HazelcastQueue;
import com.openlattice.neuron.signals.Signal;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

public class SignalQueueReceptor implements Receptor {

    private static final Logger logger = LoggerFactory.getLogger( SignalQueueReceptor.class );

    private final HazelcastInstance hazelcastInstance;

    public SignalQueueReceptor( HazelcastInstance hazelcastInstance ) {

        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public void process( Signal signal ) {

        IQueue<Signal> queue = hazelcastInstance.getQueue( HazelcastQueue.SIGNAL.name() );

        try {
            queue.put( signal );
        } catch ( InterruptedException e ) {
            logger.error( e.getMessage(), e );
        }
    }
}
