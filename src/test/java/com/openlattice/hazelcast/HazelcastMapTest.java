package com.openlattice.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.configuration.IMockitoConfiguration;

public class HazelcastMapTest {
    @Test
    public void testDuplicateRegistration() {
        String uniqueId = "46587ede-b14a-46de-b8a2-1e00b6fe2ae3";
        HazelcastInstance instance = Mockito.mock(HazelcastInstance.class);
        IMap<Object, Object> map = Mockito.mock(IMap.class);
        Mockito.when(instance.getMap(uniqueId)).thenReturn(map);

        HazelcastMap<Object, Object> first = new HazelcastMap<>(uniqueId);
        IMap<Object, Object> value = first.getMap( instance );
        Assert.assertEquals(map, value);

        HazelcastMap<Object, Object> second = new HazelcastMap<>(uniqueId);
        try {
            second.getMap( instance );
            Assert.fail("Expected an IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        } catch(Exception e) {
            Assert.fail(String.format("Expected IllegalStateException but got {}", e));
        }
        Assert.assertEquals(map, value);

    }
}
