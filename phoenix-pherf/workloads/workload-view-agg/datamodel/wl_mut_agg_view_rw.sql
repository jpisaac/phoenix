/*
Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
CREATE VIEW IF NOT EXISTS PHERF.MUT_AGGREGATION_VIEW (
    HOST CHAR(2) NOT NULL,
    DOMAIN VARCHAR NOT NULL,
    FEATURE VARCHAR NOT NULL,
    DATE DATE NOT NULL,
    CORE BIGINT,
    DB BIGINT,
    ACTIVE_VISITOR INTEGER
    CONSTRAINT PK PRIMARY KEY
    (
        HOST,
        DOMAIN,
        FEATURE,
        DATE
    )
) AS SELECT * FROM PHERF.MUTABLE_ENTITY_DATA WHERE PARTITION_KEY = 'AGG'
