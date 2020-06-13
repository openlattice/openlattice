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

package com.openlattice.datastore.test;

import java.util.Map.Entry;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.AbstractEntryProcessor;

public class HazelcastTestHarness {

	private static HazelcastInstance hazelcast = null;

	@BeforeClass
	public static void initHazelcast() {
		if (hazelcast == null) {
			Config config = new Config("test");
			config.setGroupConfig(new GroupConfig("test", "rogue"));
			config.setNetworkConfig(new NetworkConfig().setPort(5801).setPortAutoIncrement(true)
					.setJoin(new JoinConfig().setMulticastConfig(new MulticastConfig().setEnabled(false))));

			hazelcast = Hazelcast.newHazelcastInstance(config);
		}

	}

	@AfterClass
	public static void shutdownHazelcast() {
		hazelcast.shutdown();
		hazelcast = null;
	}

	@Test
	public void simpleTest() {
		IMap<String, String> test = hazelcast.getMap("test");

		test.put("world", "hello");
		test.put("hello", "world");
		test.put("bye", "sam");

		test.executeOnEntries(new AbstractEntryProcessor<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Object process(Entry<String, String> entry) {
				System.out.println(entry.getValue());
				return null;
			}

		});
	}
}