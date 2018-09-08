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

package com.openlattice.datastore.odata.controllers;

import com.hazelcast.core.HazelcastInstance;
import com.openlattice.datastore.odata.EntityCollectionProcessorImpl;
import com.openlattice.datastore.odata.EntityProcessorImpl;
import com.openlattice.datastore.odata.EdmProviderImpl;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.ODataStorageService;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;

import java.util.ArrayList;
import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.olingo.commons.api.edmx.EdmxReference;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ServiceMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ODataController {
    private static final Logger logger = LoggerFactory.getLogger(ODataController.class);

    @Inject
    private HazelcastInstance hazelcast;

    @Inject
    private EdmManager dms;

    @Inject
    private HazelcastSchemaManager schemaManager;

    @Inject
    private ODataStorageService storage;


    @RequestMapping({"", "/*"})
    public void handleOData(HttpServletRequest req, HttpServletResponse resp) throws ServletException {
        try {
            // create odata handler and configure it with CsdlEdmProvider and Processor
            OData odata = OData.newInstance();
            ServiceMetadata edm = odata.createServiceMetadata(new EdmProviderImpl(dms, schemaManager),
                    new ArrayList<EdmxReference>());
            ODataHttpHandler handler = odata.createHandler(edm);
            handler.register(new EntityCollectionProcessorImpl(storage));
            handler.register(new EntityProcessorImpl(storage));
            // let the handler do the work
            handler.process(req, resp);
        } catch (RuntimeException e) {
            logger.error("Server Error occurred in ExampleServlet", e);
            throw new ServletException(e);
        }
    }
}