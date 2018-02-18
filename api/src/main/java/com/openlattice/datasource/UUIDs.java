/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.datasource;

import java.util.UUID;

public class UUIDs {
    public static enum Syncs {
        BASE( new UUID( 0, 0 ) );
        private final UUID syncId;

        private Syncs( UUID syncId ) {
            this.syncId = syncId;
        }

        public UUID getSyncId() {
            return syncId;
        }
    }
}
