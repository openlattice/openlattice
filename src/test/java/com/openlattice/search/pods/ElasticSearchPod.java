package com.openlattice.search.pods;

import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.search.EsEdmService;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doNothing;

@Configuration
public class ElasticSearchPod {

    @Bean
    public EsEdmService esEdmService() {
        EsEdmService mockEsEdmService = Mockito.mock(EsEdmService.class);
        doNothing().when(mockEsEdmService).createEntitySet(isA(EntitySet.class), isA(List.class) );
        doNothing().when(mockEsEdmService).createPropertyType(isA(PropertyType.class));

        return mockEsEdmService;
    }
}
