/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end.json;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.index.SingleCellIndexIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_ENCODED_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_STORAGE_SCHEME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonValueIT extends ParallelStatsDisabledIT {
    private String JsonDoc1 = "{  \n" +
            "     \"info\":{    \n" +
            "       \"type\":1,  \n" +
            "       \"address\":{    \n" +
            "         \"town\":\"Bristol\",  \n" +
            "         \"county\":\"Avon\",  \n" +
            "         \"country\":\"England\"  \n" +
            "       },  \n" +
            "       \"tags\":[\"Sport\", \"Water polo\"]  \n" +
            "    },  \n" +
            "    \"type\":\"Basic\",  \n" +
            "    \"name\":\"AndersenFamily\"  \n" +
            " }";
    private String JsonDoc4 = "{  \n" +
        "     \"info\":{    \n" +
        "       \"type\":1,  \n" +
        "       \"address\":{    \n" +
        "         \"town\":\"Manchester\",  \n" +
        "         \"county\":\"Avon\",  \n" +
        "         \"country\":\"England\"  \n" +
        "       },  \n" +
        "       \"tags\":[\"Sport\", \"Water polo\"]  \n" +
        "    },  \n" +
        "    \"type\":\"Basic\",  \n" +
        "    \"name\":\"AndersenFamily\"  \n" +
        " }";

    private String JsonDoc2="{\n" +
            "   \"testCnt\": \"SomeCnt1\",                    \n" +
            "   \"test\": \"test1\",\n" +
            "   \"batchNo\": 1,\n" +
            "   \"infoTop\":[\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e407a8dbd65781450\",\n" +
            "                       \"index\": 0,\n" +
            "                       \"guid\": \"4f5a46f2-7271-492a-8347-a8223516715f\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$3,746.11\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 20,\n" +
            "                       \"eyeColor\": \"green\",\n" +
            "                       \"name\": \"Castaneda Golden\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"AUSTEX\",\n" +
            "                       \"email\": \"castanedagolden@austex.com\",\n" +
            "                       \"phone\": \"+1 (979) 486-3061\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Urbana\",\n" +
            "                       \"state\": \"Delaware\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"322 Hancock Street, Nicut, Georgia, 5007\",\n" +
            "                       \"about\": \"Esse anim minim nostrud aliquip. Quis anim ex dolore magna exercitation deserunt minim ad do est non. Magna fugiat eiusmod incididunt cupidatat. Anim occaecat nulla cillum culpa sunt amet.\\r\\n\",\n" +
            "                       \"registered\": \"2015-11-06T01:32:28 +08:00\",\n" +
            "                       \"latitude\": 83.51654,\n" +
            "                       \"longitude\": -93.749216,\n" +
            "                       \"tags\": [\n" +
            "                       \"incididunt\",\n" +
            "                       \"nostrud\",\n" +
            "                       \"incididunt\",\n" +
            "                       \"Lorem\",\n" +
            "                       \"mollit\",\n" +
            "                       \"tempor\",\n" +
            "                       \"incididunt\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Cortez Bowman\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Larsen Wolf\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Colon Rivers\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Castaneda Golden! You have 10 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982ef091f4785f15251f\",\n" +
            "                       \"index\": 1,\n" +
            "                       \"guid\": \"bcfc487d-de23-4721-86bd-809d37a007c2\",\n" +
            "                       \"isActive\": false,\n" +
            "                       \"balance\": \"$1,539.97\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 31,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Jackson Dillard\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"QUONATA\",\n" +
            "                       \"email\": \"jacksondillard@quonata.com\",\n" +
            "                       \"phone\": \"+1 (950) 552-3553\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Cetronia\",\n" +
            "                       \"state\": \"Massachusetts\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"848 Hampton Avenue, Shasta, Marshall Islands, 6596\",\n" +
            "                       \"about\": \"Mollit nisi cillum sunt aliquip. Est ex nisi deserunt aliqua anim nisi dolor. Ullamco est consectetur deserunt do voluptate excepteur esse reprehenderit laboris officia. Deserunt sint velit mollit aliquip amet ad in tempor excepteur magna proident Lorem reprehenderit consequat.\\r\\n\",\n" +
            "                       \"registered\": \"2018-05-13T10:54:03 +07:00\",\n" +
            "                       \"latitude\": -68.213281,\n" +
            "                       \"longitude\": -147.388909,\n" +
            "                       \"tags\": [\n" +
            "                       \"adipisicing\",\n" +
            "                       \"Lorem\",\n" +
            "                       \"sit\",\n" +
            "                       \"voluptate\",\n" +
            "                       \"cupidatat\",\n" +
            "                       \"deserunt\",\n" +
            "                       \"consectetur\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Casandra Best\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Lauri Santiago\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Maricela Foster\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Jackson Dillard! You have 4 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"strawberry\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982eecb0f6158d7415b7\",\n" +
            "                       \"index\": 2,\n" +
            "                       \"guid\": \"09b31b54-6341-4a7e-8e58-bec0f766d5f4\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$1,357.52\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 20,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Battle Washington\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"ONTALITY\",\n" +
            "                       \"email\": \"battlewashington@ontality.com\",\n" +
            "                       \"phone\": \"+1 (934) 429-3950\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Windsor\",\n" +
            "                       \"state\": \"Virginia\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"299 Campus Place, Innsbrook, Nevada, 4795\",\n" +
            "                       \"about\": \"Consequat voluptate nisi duis nostrud anim cupidatat officia dolore non velit Lorem. Pariatur sit consectetur do reprehenderit irure Lorem consectetur ad nostrud. Dolore tempor est fugiat officia ad nostrud. Cupidatat quis aute consectetur Lorem. Irure qui tempor deserunt nisi quis quis culpa veniam cillum est. Aute consequat pariatur ut minim sunt.\\r\\n\",\n" +
            "                       \"registered\": \"2018-12-07T03:42:53 +08:00\",\n" +
            "                       \"latitude\": -6.967753,\n" +
            "                       \"longitude\": 64.796997,\n" +
            "                       \"tags\": [\n" +
            "                       \"in\",\n" +
            "                       \"do\",\n" +
            "                       \"labore\",\n" +
            "                       \"laboris\",\n" +
            "                       \"dolore\",\n" +
            "                       \"est\",\n" +
            "                       \"nisi\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Faye Decker\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Judy Skinner\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Angie Faulkner\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Battle Washington! You have 2 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e1298ef388f75cda0\",\n" +
            "                       \"index\": 3,\n" +
            "                       \"guid\": \"deebe756-c9cd-43f5-9dd6-bc8d2edeab01\",\n" +
            "                       \"isActive\": false,\n" +
            "                       \"balance\": \"$3,684.61\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 27,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Watkins Aguirre\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"WAAB\",\n" +
            "                       \"email\": \"watkinsaguirre@waab.com\",\n" +
            "                       \"phone\": \"+1 (861) 526-2440\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Healy\",\n" +
            "                       \"state\": \"Nebraska\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"245 Bouck Court, Malo, Minnesota, 8990\",\n" +
            "                       \"about\": \"Elit fugiat aliquip occaecat nostrud deserunt eu in ut et officia pariatur ipsum non. Dolor exercitation irure cupidatat velit eiusmod voluptate esse enim. Minim aliquip do ut esse irure commodo duis aliquip deserunt ea enim incididunt. Consequat Lorem id duis occaecat proident mollit ad officia fugiat. Nostrud irure deserunt commodo consectetur cillum. Quis qui eiusmod ullamco exercitation amet do occaecat sint laboris ut laboris amet. Elit consequat fugiat cupidatat enim occaecat ullamco.\\r\\n\",\n" +
            "                       \"registered\": \"2021-05-27T03:15:12 +07:00\",\n" +
            "                       \"latitude\": 86.552038,\n" +
            "                       \"longitude\": 175.688809,\n" +
            "                       \"tags\": [\n" +
            "                       \"nostrud\",\n" +
            "                       \"et\",\n" +
            "                       \"ullamco\",\n" +
            "                       \"aliqua\",\n" +
            "                       \"minim\",\n" +
            "                       \"tempor\",\n" +
            "                       \"proident\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Dionne Lindsey\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Bonner Logan\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Neal Case\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Watkins Aguirre! You have 5 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"strawberry\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e3cb0317d825dfbb5\",\n" +
            "                       \"index\": 4,\n" +
            "                       \"guid\": \"ac778765-da9a-4923-915b-1b967e1bee96\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$2,787.54\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 34,\n" +
            "                       \"eyeColor\": \"green\",\n" +
            "                       \"name\": \"Barbra Fry\",\n" +
            "                       \"gender\": \"female\",\n" +
            "                       \"company\": \"SPACEWAX\",\n" +
            "                       \"email\": \"barbrafry@spacewax.com\",\n" +
            "                       \"phone\": \"+1 (895) 538-2479\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Movico\",\n" +
            "                       \"state\": \"Pennsylvania\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"812 Losee Terrace, Elbert, South Dakota, 9870\",\n" +
            "                       \"about\": \"Ea Lorem nisi aliqua incididunt deserunt sint. Cillum do magna sint quis enim velit cupidatat deserunt pariatur esse labore. Laborum velit nostrud in occaecat amet commodo enim ex commodo. Culpa do est sit reprehenderit nulla duis ex irure reprehenderit velit aliquip. Irure et eiusmod ad minim laborum ut fugiat dolore in anim mollit aliquip aliqua sunt. Commodo Lorem anim magna eiusmod.\\r\\n\",\n" +
            "                       \"registered\": \"2020-05-05T05:27:59 +07:00\",\n" +
            "                       \"latitude\": -55.592888,\n" +
            "                       \"longitude\": 68.056625,\n" +
            "                       \"tags\": [\n" +
            "                       \"magna\",\n" +
            "                       \"sint\",\n" +
            "                       \"minim\",\n" +
            "                       \"dolore\",\n" +
            "                       \"ad\",\n" +
            "                       \"exercitation\",\n" +
            "                       \"laborum\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Mccullough Roman\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Lang Morales\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Luann Carrillo\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Barbra Fry! You have 6 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e44e4e11611e5f62a\",\n" +
            "                       \"index\": 5,\n" +
            "                       \"guid\": \"d02e17de-fed9-4839-8d75-e8d05fe68c94\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$1,023.39\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 38,\n" +
            "                       \"eyeColor\": \"green\",\n" +
            "                       \"name\": \"Byers Grant\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"ZAGGLES\",\n" +
            "                       \"email\": \"byersgrant@zaggles.com\",\n" +
            "                       \"phone\": \"+1 (992) 570-3190\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Chamberino\",\n" +
            "                       \"state\": \"North Dakota\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"826 Cumberland Street, Shaft, Washington, 424\",\n" +
            "                       \"about\": \"Deserunt tempor sint culpa in ex occaecat quis exercitation voluptate mollit occaecat officia. Aute aliquip officia id cupidatat non consectetur nulla mollit laborum ex mollit culpa exercitation. Aute nisi ullamco adipisicing sit proident proident duis. Exercitation ex id id enim cupidatat pariatur amet reprehenderit fugiat ea.\\r\\n\",\n" +
            "                       \"registered\": \"2017-10-12T04:55:42 +07:00\",\n" +
            "                       \"latitude\": -26.03892,\n" +
            "                       \"longitude\": -35.959528,\n" +
            "                       \"tags\": [\n" +
            "                       \"et\",\n" +
            "                       \"adipisicing\",\n" +
            "                       \"excepteur\",\n" +
            "                       \"do\",\n" +
            "                       \"ad\",\n" +
            "                       \"exercitation\",\n" +
            "                       \"commodo\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Louise Clarke\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Pratt Velazquez\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Violet Reyes\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Byers Grant! You have 8 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982ef6ed0ffe65e0f414\",\n" +
            "                       \"index\": 6,\n" +
            "                       \"guid\": \"37f92715-a4d1-476e-98d9-b4901426c5ea\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$2,191.12\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 33,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Rasmussen Todd\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"ROUGHIES\",\n" +
            "                       \"email\": \"rasmussentodd@roughies.com\",\n" +
            "                       \"phone\": \"+1 (893) 420-3792\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Floriston\",\n" +
            "                       \"state\": \"Indiana\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"295 McClancy Place, Berlin, Federated States Of Micronesia, 303\",\n" +
            "                       \"about\": \"Est cillum fugiat reprehenderit minim minim esse qui. Eiusmod quis pariatur adipisicing sunt ipsum duis dolor veniam. Aliqua ex cupidatat officia exercitation sint duis exercitation ut. Cillum magna laboris id Lorem mollit consequat ex anim voluptate Lorem enim et velit nulla. Non consectetur incididunt id et ad tempor amet elit tempor aliquip velit incididunt esse adipisicing. Culpa pariatur est occaecat voluptate. Voluptate pariatur pariatur esse cillum proident eiusmod duis proident minim magna sit voluptate exercitation est.\\r\\n\",\n" +
            "                       \"registered\": \"2015-10-10T12:39:42 +07:00\",\n" +
            "                       \"latitude\": -20.559815,\n" +
            "                       \"longitude\": 28.453852,\n" +
            "                       \"tags\": [\n" +
            "                       \"reprehenderit\",\n" +
            "                       \"velit\",\n" +
            "                       \"non\",\n" +
            "                       \"non\",\n" +
            "                       \"veniam\",\n" +
            "                       \"laborum\",\n" +
            "                       \"duis\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Stark Carney\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Price Roberts\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Lillian Henry\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Rasmussen Todd! You have 3 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       }\n" +
            "   ]\n" +
            "}";

    private String JsonDoc3="{\n" +
            "   \"testCnt\": \"SomeCnt2\",                    \n" +
            "   \"test\": \"test2\",\n" +
            "   \"batchNo\": 2,\n" +
            "   \"infoTop\":[\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e407a8dbd65781450\",\n" +
            "                       \"index\": 0,\n" +
            "                       \"guid\": \"4f5a46f2-7271-492a-8347-a8223516715f\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$3,746.11\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 20,\n" +
            "                       \"eyeColor\": \"green\",\n" +
            "                       \"name\": \"Castaneda Golden\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"AUSTEX\",\n" +
            "                       \"email\": \"castanedagolden@austex.com\",\n" +
            "                       \"phone\": \"+1 (979) 486-3061\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Urbana\",\n" +
            "                       \"state\": \"Delaware\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"322 Hancock Street, Nicut, Georgia, 5007\",\n" +
            "                       \"about\": \"Esse anim minim nostrud aliquip. Quis anim ex dolore magna exercitation deserunt minim ad do est non. Magna fugiat eiusmod incididunt cupidatat. Anim occaecat nulla cillum culpa sunt amet.\\r\\n\",\n" +
            "                       \"registered\": \"2015-11-06T01:32:28 +08:00\",\n" +
            "                       \"latitude\": 83.51654,\n" +
            "                       \"longitude\": -93.749216,\n" +
            "                       \"tags\": [\n" +
            "                       \"incididunt\",\n" +
            "                       \"nostrud\",\n" +
            "                       \"incididunt\",\n" +
            "                       \"Lorem\",\n" +
            "                       \"mollit\",\n" +
            "                       \"tempor\",\n" +
            "                       \"incididunt\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Cortez Bowman\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Larsen Wolf\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Colon Rivers\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Castaneda Golden! You have 10 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982ef091f4785f15251f\",\n" +
            "                       \"index\": 1,\n" +
            "                       \"guid\": \"bcfc487d-de23-4721-86bd-809d37a007c2\",\n" +
            "                       \"isActive\": false,\n" +
            "                       \"balance\": \"$1,539.97\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 31,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Jackson Dillard\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"QUONATA\",\n" +
            "                       \"email\": \"jacksondillard@quonata.com\",\n" +
            "                       \"phone\": \"+1 (950) 552-3553\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Cetronia\",\n" +
            "                       \"state\": \"Massachusetts\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"848 Hampton Avenue, Shasta, Marshall Islands, 6596\",\n" +
            "                       \"about\": \"Mollit nisi cillum sunt aliquip. Est ex nisi deserunt aliqua anim nisi dolor. Ullamco est consectetur deserunt do voluptate excepteur esse reprehenderit laboris officia. Deserunt sint velit mollit aliquip amet ad in tempor excepteur magna proident Lorem reprehenderit consequat.\\r\\n\",\n" +
            "                       \"registered\": \"2018-05-13T10:54:03 +07:00\",\n" +
            "                       \"latitude\": -68.213281,\n" +
            "                       \"longitude\": -147.388909,\n" +
            "                       \"tags\": [\n" +
            "                       \"adipisicing\",\n" +
            "                       \"Lorem\",\n" +
            "                       \"sit\",\n" +
            "                       \"voluptate\",\n" +
            "                       \"cupidatat\",\n" +
            "                       \"deserunt\",\n" +
            "                       \"consectetur\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Casandra Best\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Lauri Santiago\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Maricela Foster\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Jackson Dillard! You have 4 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"strawberry\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982eecb0f6158d7415b7\",\n" +
            "                       \"index\": 2,\n" +
            "                       \"guid\": \"09b31b54-6341-4a7e-8e58-bec0f766d5f4\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$1,357.52\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 20,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Battle Washington\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"ONTALITY\",\n" +
            "                       \"email\": \"battlewashington@ontality.com\",\n" +
            "                       \"phone\": \"+1 (934) 429-3950\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Windsor\",\n" +
            "                       \"state\": \"Virginia\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"299 Campus Place, Innsbrook, Nevada, 4795\",\n" +
            "                       \"about\": \"Consequat voluptate nisi duis nostrud anim cupidatat officia dolore non velit Lorem. Pariatur sit consectetur do reprehenderit irure Lorem consectetur ad nostrud. Dolore tempor est fugiat officia ad nostrud. Cupidatat quis aute consectetur Lorem. Irure qui tempor deserunt nisi quis quis culpa veniam cillum est. Aute consequat pariatur ut minim sunt.\\r\\n\",\n" +
            "                       \"registered\": \"2018-12-07T03:42:53 +08:00\",\n" +
            "                       \"latitude\": -6.967753,\n" +
            "                       \"longitude\": 64.796997,\n" +
            "                       \"tags\": [\n" +
            "                       \"in\",\n" +
            "                       \"do\",\n" +
            "                       \"labore\",\n" +
            "                       \"laboris\",\n" +
            "                       \"dolore\",\n" +
            "                       \"est\",\n" +
            "                       \"nisi\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Faye Decker\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Judy Skinner\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Angie Faulkner\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Battle Washington! You have 2 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e1298ef388f75cda0\",\n" +
            "                       \"index\": 3,\n" +
            "                       \"guid\": \"deebe756-c9cd-43f5-9dd6-bc8d2edeab01\",\n" +
            "                       \"isActive\": false,\n" +
            "                       \"balance\": \"$3,684.61\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 27,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Watkins Aguirre\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"WAAB\",\n" +
            "                       \"email\": \"watkinsaguirre@waab.com\",\n" +
            "                       \"phone\": \"+1 (861) 526-2440\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Healy\",\n" +
            "                       \"state\": \"Nebraska\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"245 Bouck Court, Malo, Minnesota, 8990\",\n" +
            "                       \"about\": \"Elit fugiat aliquip occaecat nostrud deserunt eu in ut et officia pariatur ipsum non. Dolor exercitation irure cupidatat velit eiusmod voluptate esse enim. Minim aliquip do ut esse irure commodo duis aliquip deserunt ea enim incididunt. Consequat Lorem id duis occaecat proident mollit ad officia fugiat. Nostrud irure deserunt commodo consectetur cillum. Quis qui eiusmod ullamco exercitation amet do occaecat sint laboris ut laboris amet. Elit consequat fugiat cupidatat enim occaecat ullamco.\\r\\n\",\n" +
            "                       \"registered\": \"2021-05-27T03:15:12 +07:00\",\n" +
            "                       \"latitude\": 86.552038,\n" +
            "                       \"longitude\": 175.688809,\n" +
            "                       \"tags\": [\n" +
            "                       \"nostrud\",\n" +
            "                       \"et\",\n" +
            "                       \"ullamco\",\n" +
            "                       \"aliqua\",\n" +
            "                       \"minim\",\n" +
            "                       \"tempor\",\n" +
            "                       \"proident\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Dionne Lindsey\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Bonner Logan\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Neal Case\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Watkins Aguirre! You have 5 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"strawberry\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e3cb0317d825dfbb5\",\n" +
            "                       \"index\": 4,\n" +
            "                       \"guid\": \"ac778765-da9a-4923-915b-1b967e1bee96\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$2,787.54\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 34,\n" +
            "                       \"eyeColor\": \"green\",\n" +
            "                       \"name\": \"Barbra Fry\",\n" +
            "                       \"gender\": \"female\",\n" +
            "                       \"company\": \"SPACEWAX\",\n" +
            "                       \"email\": \"barbrafry@spacewax.com\",\n" +
            "                       \"phone\": \"+1 (895) 538-2479\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Movico\",\n" +
            "                       \"state\": \"Pennsylvania\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"812 Losee Terrace, Elbert, South Dakota, 9870\",\n" +
            "                       \"about\": \"Ea Lorem nisi aliqua incididunt deserunt sint. Cillum do magna sint quis enim velit cupidatat deserunt pariatur esse labore. Laborum velit nostrud in occaecat amet commodo enim ex commodo. Culpa do est sit reprehenderit nulla duis ex irure reprehenderit velit aliquip. Irure et eiusmod ad minim laborum ut fugiat dolore in anim mollit aliquip aliqua sunt. Commodo Lorem anim magna eiusmod.\\r\\n\",\n" +
            "                       \"registered\": \"2020-05-05T05:27:59 +07:00\",\n" +
            "                       \"latitude\": -55.592888,\n" +
            "                       \"longitude\": 68.056625,\n" +
            "                       \"tags\": [\n" +
            "                       \"magna\",\n" +
            "                       \"sint\",\n" +
            "                       \"minim\",\n" +
            "                       \"dolore\",\n" +
            "                       \"ad\",\n" +
            "                       \"exercitation\",\n" +
            "                       \"laborum\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Mccullough Roman\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Lang Morales\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Luann Carrillo\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Barbra Fry! You have 6 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e44e4e11611e5f62a\",\n" +
            "                       \"index\": 5,\n" +
            "                       \"guid\": \"d02e17de-fed9-4839-8d75-e8d05fe68c94\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$1,023.39\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 38,\n" +
            "                       \"eyeColor\": \"green\",\n" +
            "                       \"name\": \"Byers Grant\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"ZAGGLES\",\n" +
            "                       \"email\": \"byersgrant@zaggles.com\",\n" +
            "                       \"phone\": \"+1 (992) 570-3190\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Chamberino\",\n" +
            "                       \"state\": \"North Dakota\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"826 Cumberland Street, Shaft, Washington, 424\",\n" +
            "                       \"about\": \"Deserunt tempor sint culpa in ex occaecat quis exercitation voluptate mollit occaecat officia. Aute aliquip officia id cupidatat non consectetur nulla mollit laborum ex mollit culpa exercitation. Aute nisi ullamco adipisicing sit proident proident duis. Exercitation ex id id enim cupidatat pariatur amet reprehenderit fugiat ea.\\r\\n\",\n" +
            "                       \"registered\": \"2017-10-12T04:55:42 +07:00\",\n" +
            "                       \"latitude\": -26.03892,\n" +
            "                       \"longitude\": -35.959528,\n" +
            "                       \"tags\": [\n" +
            "                       \"et\",\n" +
            "                       \"adipisicing\",\n" +
            "                       \"excepteur\",\n" +
            "                       \"do\",\n" +
            "                       \"ad\",\n" +
            "                       \"exercitation\",\n" +
            "                       \"commodo\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Louise Clarke\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Pratt Velazquez\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Violet Reyes\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Byers Grant! You have 8 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982ef6ed0ffe65e0f414\",\n" +
            "                       \"index\": 6,\n" +
            "                       \"guid\": \"37f92715-a4d1-476e-98d9-b4901426c5ea\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$2,191.12\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 33,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Rasmussen Todd\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"ROUGHIES\",\n" +
            "                       \"email\": \"rasmussentodd@roughies.com\",\n" +
            "                       \"phone\": \"+1 (893) 420-3792\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Floriston\",\n" +
            "                       \"state\": \"Indiana\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"295 McClancy Place, Berlin, Federated States Of Micronesia, 303\",\n" +
            "                       \"about\": \"Est cillum fugiat reprehenderit minim minim esse qui. Eiusmod quis pariatur adipisicing sunt ipsum duis dolor veniam. Aliqua ex cupidatat officia exercitation sint duis exercitation ut. Cillum magna laboris id Lorem mollit consequat ex anim voluptate Lorem enim et velit nulla. Non consectetur incididunt id et ad tempor amet elit tempor aliquip velit incididunt esse adipisicing. Culpa pariatur est occaecat voluptate. Voluptate pariatur pariatur esse cillum proident eiusmod duis proident minim magna sit voluptate exercitation est.\\r\\n\",\n" +
            "                       \"registered\": \"2015-10-10T12:39:42 +07:00\",\n" +
            "                       \"latitude\": -20.559815,\n" +
            "                       \"longitude\": 28.453852,\n" +
            "                       \"tags\": [\n" +
            "                       \"reprehenderit\",\n" +
            "                       \"velit\",\n" +
            "                       \"non\",\n" +
            "                       \"non\",\n" +
            "                       \"veniam\",\n" +
            "                       \"laborum\",\n" +
            "                       \"duis\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Stark Carney\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Price Roberts\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Lillian Henry\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Rasmussen Todd! You have 3 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       }\n" +
            "   ]\n" +
            "}";

    @Test
    public void testSimpleJsonQuery() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol json)";
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (1,2, '" + JsonDoc1 + "')");
            conn.commit();

            String queryTemplate ="SELECT JSON_VALUE(jsoncol, '$.type'), JSON_VALUE(jsoncol, '$.info.address.town'), " +
                    "JSON_VALUE(jsoncol, '$.info.tags[1]'), JSON_VALUE(jsoncol, '$.info.tags'), JSON_VALUE(jsoncol, '$.info') " +
                    " FROM " + tableName +
                    " WHERE JSON_VALUE(jsoncol, '$.name') = '%s'";
            String query = String.format(queryTemplate, "AndersenFamily");
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("Basic", rs.getString(1));
            assertEquals("Bristol", rs.getString(2));
            assertEquals("Water polo", rs.getString(3));
            assertEquals("[\"Sport\",\"Water polo\"]", rs.getString(4));
            assertEquals("{\"type\":1,\"address\":{\"town\":\"Bristol\",\"county\":\"Avon\",\"country\":\"England\"},\"tags\":[\"Sport\",\"Water polo\"]}", rs.getString(5));
            assertFalse(rs.next());

            // Now check for empty match
            query = String.format(queryTemplate, "Windsors");
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSimpleJsonModify() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol json)";
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (1,2, '" + JsonDoc1 + "')");
            conn.commit();

            String upsert ="UPSERT INTO " + tableName + " VALUES(1,2, JSON_MODIFY(jsoncol, '$.info.address.town', 'Manchester')) ";
            conn.createStatement().execute(upsert);
            conn.commit();
            SingleCellIndexIT.dumpTable(tableName);
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1,2, JSON_MODIFY(jsoncol, '$.info.tags', '[\"Sport\", \"Water polo\", \"Books\"]')) ");
            conn.commit();

            String queryTemplate ="SELECT JSON_VALUE(jsoncol, '$.type'), JSON_VALUE(jsoncol, '$.info.address.town'), " +
                    "JSON_VALUE(jsoncol, '$.info.tags[1]'), JSON_VALUE(jsoncol, '$.info.tags'), JSON_VALUE(jsoncol, '$.info') " +
                    " FROM " + tableName +
                    " WHERE JSON_VALUE(jsoncol, '$.name') = '%s'";
            String query = String.format(queryTemplate, "AndersenFamily");
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("Basic", rs.getString(1));
            assertEquals("Manchester", rs.getString(2));
            assertEquals("Water polo", rs.getString(3));
            assertEquals("[\"Sport\",\"Water polo\",\"Books\"]", rs.getString(4));
            assertEquals("{\"type\":1,\"address\":{\"town\":\"Manchester\",\"county\":\"Avon\",\"country\":\"England\"},\"tags\":[\"Sport\",\"Water polo\",\"Books\"]}", rs.getString(5));
            assertFalse(rs.next());

            // Now check for empty match
            query = String.format(queryTemplate, "Windsors");
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSimpleJsonQueryDC() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            //String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol.v varchar)";
            String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol.jsoncol jsondc)";
            conn.createStatement().execute(ddl);
            //conn.createStatement().execute("UPSERT INTO " + tableName + " (pk, col, jsoncol.type varchar) VALUES (1,2, '" + JsonDoc1 + "')");
            conn.createStatement().execute("UPSERT INTO " + tableName + " (pk, col, jsoncol) VALUES (1,2, '" + JsonDoc1 + "')");

            SingleCellIndexIT.dumpTable(tableName);

            String queryTemplate ="SELECT JSON_VALUE_DC(jsoncol, '$.type'), JSON_VALUE_DC(jsoncol, '$.info.address.town') " +
                    ", JSON_VALUE_DC(jsoncol, '$.info.tags[1]'), JSON_VALUE_DC(jsoncol, '$.info.tags'),JSON_QUERY_DC(jsoncol, '$.info') " +
                    " FROM " + tableName
                    + " WHERE JSON_VALUE_DC(jsoncol, '$.name') = '%s'";

            String query = String.format(queryTemplate, "AndersenFamily");

            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("Basic", rs.getString(1));
            assertEquals("Bristol", rs.getString(2));
            assertEquals("Water polo", rs.getString(3));
            assertEquals("[\"Sport\",\"Water polo\"]", rs.getString(4));
            assertEquals("{\"type\":1,\"address\":{\"town\":\"Bristol\",\"county\":\"Avon\",\"country\":\"England\"},\"tags\":[\"Sport\",\"Water polo\"]}", rs.getString(5));
            assertFalse(rs.next());

            // Now check for empty match
            query = String.format(queryTemplate, "Windsors");
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSimpleJsonQueryDC2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            //String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol.v varchar)";
            String ddl = "create table if not exists " + tableName + " (pk integer primary key, col integer, jsoncol.jsoncol jsondc)";
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("UPSERT INTO " + tableName + " (pk, col, jsoncol) VALUES (1,2, '" + JsonDoc2 + "')");
            SingleCellIndexIT.dumpTable(tableName);

            ResultSet rs = conn.createStatement().executeQuery("SELECT JSON_VALUE_DC(JSONCOL,'$.test'), JSON_VALUE_DC(JSONCOL, '$.testCnt'), JSON_VALUE_DC(JSONCOL, '$.infoTop[5].info.address.state'),JSON_VALUE_DC(JSONCOL, '$.infoTop[4].tags[1]')  FROM "
                    + tableName + " WHERE JSON_VALUE_DC(JSONCOL, '.test')='test1'");
            assertTrue(rs.next());
            assertEquals("test1", rs.getString(1));
            assertEquals("SomeCnt1", rs.getString(2));

            conn.createStatement().execute("create table if not exists TBL_JSON_DC (pk integer primary key, col integer, jsoncol.jsoncol jsondc)");
            upsertScenario(conn.unwrap(PhoenixConnection.class), "DC");
            SingleCellIndexIT.dumpTable("TBL_JSON_DC");
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            ResultSet rsD = databaseMetaData.getColumns(null,null, tableName.toUpperCase(), "JSONCOL%");
            rsD.next();
            assertEquals("JSONDC", rsD.getString("TYPE_NAME"));
            fail();
        }
    }

    @Test
    public void testExpressionIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String ddl = "create table if not exists " + tableName + " (pk integer primary key, col integer, jsoncol.jsoncol json)";
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("UPSERT INTO " + tableName + " (pk, col, jsoncol) VALUES (1,2, '" + JsonDoc1+ "')");

            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName+ " (JSON_VALUE(JSONCOL,'$.test') include (col1)");
            SingleCellIndexIT.dumpTable(indexName);
            SingleCellIndexIT.dumpTable("SYSTEM.CATALOG");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.DIVIDE_BY_ZERO.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testExpressionIndex1() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("CREATE TABLE " + tableName + " (k1 INTEGER PRIMARY KEY, k2 integer, k3 varchar)");

            conn.createStatement().execute("UPSERT into " + tableName +" VALUES (1, 10, 'ten')");
            conn.createStatement().execute("UPSERT into " + tableName +" VALUES (2, 4, 'four')");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName+ " (k2*2) include (k3)");
            SingleCellIndexIT.dumpTable(indexName);
            SingleCellIndexIT.dumpTable("SYSTEM.CATALOG");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.DIVIDE_BY_ZERO.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSimpleJsonModifyDC() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol.jsoncol jsondc)";
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (1,2, '" + JsonDoc1 + "')");
            SingleCellIndexIT.dumpTable(tableName);

            String upsert ="UPSERT INTO " + tableName + " VALUES(1,2, JSON_MODIFY_DC(jsoncol, '$.info.address.town', 'Manchester')) ";
            conn.createStatement().execute(upsert);
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1,2, JSON_MODIFY_DC(jsoncol, '$.info.tags', '[\"Sport\", \"Water polo\", \"Books\"]')) ");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1,2, JSON_MODIFY_DC(jsoncol, '$.type', 'Advanced')) ");
            SingleCellIndexIT.dumpTable(tableName);

            String queryTemplate ="SELECT JSON_VALUE_DC(jsoncol, '$.type'), JSON_VALUE_DC(jsoncol, '$.info.address.town'), " +
                    "JSON_VALUE_DC(jsoncol, '$.info.tags[1]'), JSON_VALUE_DC(jsoncol, '$.info.tags') " +
                    " FROM " + tableName +
                    " WHERE JSON_VALUE_DC(jsoncol, '$.name') = '%s'";
            String query = String.format(queryTemplate, "AndersenFamily");
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("Advanced", rs.getString(1));
            assertEquals("Manchester", rs.getString(2));
            assertEquals("Water polo", rs.getString(3));
            assertEquals("[\"Sport\",\"Water polo\",\"Books\"]", rs.getString(4));
            assertFalse(rs.next());

            // Now check for empty match
            query = String.format(queryTemplate, "Windsors");
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSimpleJsonModifyDC2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol.jsoncol jsondc)";
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setString(3, JsonDoc2);
            stmt.execute();

            String upsert ="UPSERT INTO " + tableName + " VALUES(1,2, JSON_MODIFY_DC(JSONCOL, '$.infoTop[1].favoriteFruit', 'apple'))";
            conn.createStatement().execute(upsert);
            SingleCellIndexIT.dumpTable(tableName);

            String query ="SELECT JSON_VALUE_DC(JSONCOL, '$.infoTop[1].favoriteFruit') " +
                    " FROM " + tableName ;
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("apple", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSimpleJsonModify2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol.jsoncol json)";
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setString(3, JsonDoc2);
            stmt.execute();

            String upsert ="UPSERT INTO " + tableName + " VALUES(1,2, JSON_MODIFY(JSONCOL, '$.infoTop[1].favoriteFruit', 'apple'))";
            conn.createStatement().execute(upsert);
            SingleCellIndexIT.dumpTable(tableName);

            String query ="SELECT JSON_VALUE(JSONCOL, '$.infoTop[1].favoriteFruit') " +
                    " FROM " + tableName ;
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("apple", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSimpleJsonModifyBinary2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol.jsoncol jsonb)";
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setBytes(3, JsonDoc2.getBytes());
            stmt.execute();

            String upsert ="UPSERT INTO " + tableName + " VALUES(1,2, JSON_MODIFY_B(JSONCOL, '$.infoTop[1].favoriteFruit', 'apple'))";
            conn.createStatement().execute(upsert);
            SingleCellIndexIT.dumpTable(tableName);

            String query ="SELECT JSON_VALUE_B(JSONCOL, '$.infoTop[1].favoriteFruit') " +
                    " FROM " + tableName ;
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("apple", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSimpleJsonModifyBinary() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol.jsoncol jsonb)";
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setBytes(3, JsonDoc1.getBytes());
            stmt.execute();
            conn.commit();
            SingleCellIndexIT.dumpTable(tableName);

            String upsert ="UPSERT INTO " + tableName + " VALUES(1,2, JSON_MODIFY_B(jsoncol, '$.info.address.town', 'Manchester')) ";
            conn.createStatement().execute(upsert);
            conn.commit();
            SingleCellIndexIT.dumpTable(tableName);
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1,2, JSON_MODIFY_B(jsoncol, '$.info.tags', '[\"Sport\", \"Water polo\", \"Books\"]')) ");

            String queryTemplate ="SELECT JSON_VALUE_B(jsoncol, '$.type'), JSON_VALUE_B(jsoncol, '$.info.address.town'), " +
                    "JSON_VALUE_B(jsoncol, '$.info.tags[1]'), JSON_VALUE_B(jsoncol, '$.info.tags'), JSON_VALUE_B(jsoncol, '$.info') " +
                    " FROM " + tableName +
                    " WHERE JSON_VALUE_B(jsoncol, '$.name') = '%s'";
            String query = String.format(queryTemplate, "AndersenFamily");
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("Basic", rs.getString(1));
            assertEquals("Manchester", rs.getString(2));
            assertEquals("Water polo", rs.getString(3));
            assertEquals("[\"Sport\",\"Water polo\",\"Books\"]", rs.getString(4));
            assertEquals("{\"type\":1,\"address\":{\"town\":\"Manchester\",\"county\":\"Avon\",\"country\":\"England\"},\"tags\":[\"Sport\",\"Water polo\",\"Books\"]}", rs.getString(5));
            assertFalse(rs.next());

            // Now check for empty match
            query = String.format(queryTemplate, "Windsors");
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
        }
    }

    private void upsertScenario(PhoenixConnection conn, String scenario) throws SQLException {
        String tableName = "TBL_JSON_STRING";
        if ("DC".equals(scenario)) {
            tableName = "TBL_JSON_DC";
        } else if ("BINARY".equals(scenario)) {
            tableName = "TBL_JSON_BINARY";
        }
        String ddl = "UPSERT INTO " + tableName + " (pk, col, jsoncol) VALUES (?,?,?)";

        int totalRowCount = 1;
        int maxPkValue = 1;

        int pk = 1;
        int cnt = 1;
        for (int row=1; row<= totalRowCount ; row++) {
            long last = EnvironmentEdgeManager.currentTimeMillis();
            PreparedStatement ps = conn.prepareStatement(ddl);
            if (pk == maxPkValue) {
                pk = 1;
            }
            ps.setInt(1, pk++);
            ps.setInt(2, pk);
            if (cnt <= (totalRowCount / 2)) {
                if ("BINARY".equals(scenario)) {
                    ps.setBytes(3, JsonDoc1.getBytes());
                } else {
                    ps.setString(3, JsonDoc1);
                }
            } else {
                if ("BINARY".equals(scenario)) {
                    ps.setBytes(3, JsonDoc2.getBytes());
                } else {
                    ps.setString(3, JsonDoc2);
                }
            }
            cnt++;
            ps.execute();
            long duration = EnvironmentEdgeManager.currentTimeMillis() - last;
        }
    }

    @Test
    public void testSimpleJsonQueryBinary2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            //String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol.v varchar)";
            String ddl = "create table if not exists " + tableName + " (pk integer primary key, col integer, jsoncol.jsoncol jsonb)";
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setBytes(3, JsonDoc2.getBytes());
            stmt.execute();
            conn.commit();
            ResultSet rs = conn.createStatement().executeQuery("SELECT JSON_VALUE_B(JSONCOL,'$.test'), JSON_VALUE_B(JSONCOL, '$.testCnt'), JSON_VALUE_B(JSONCOL, '$.infoTop[5].info.address.state'),JSON_VALUE_B(JSONCOL, '$.infoTop[4].tags[1]')  FROM "
                    + tableName + " WHERE JSON_VALUE_B(JSONCOL, '$.test')='test1'");
            assertTrue(rs.next());
            assertEquals("test1", rs.getString(1));
            assertEquals("SomeCnt1", rs.getString(2));
        }
    }

    @Test
    public void testSimpleJsonBQuery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol jsonb)";
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setBytes(3, JsonDoc1.getBytes());
            stmt.execute();
            conn.commit();

            String queryTemplate ="SELECT JSON_VALUE_B(jsoncol, '$.type'), JSON_VALUE_B(jsoncol, '$.info.address.town'), " +
                    "JSON_VALUE_B(jsoncol, '$.info.tags[1]'), JSON_VALUE_B(jsoncol, '$.info.tags'), JSON_VALUE_B(jsoncol, '$.info') " +
                    " FROM " + tableName +
                    " WHERE JSON_VALUE_B(jsoncol, '$.name') = '%s'";
            String query = String.format(queryTemplate, "AndersenFamily");
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("Basic", rs.getString(1));
            assertEquals("Bristol", rs.getString(2));
            assertEquals("Water polo", rs.getString(3));
            assertEquals("[\"Sport\",\"Water polo\"]", rs.getString(4));
            assertEquals("{\"address\":{\"county\":\"Avon\",\"town\":\"Bristol\",\"country\":\"England\"},\"type\":1,\"tags\":[\"Sport\",\"Water polo\"]}", rs.getString(5));
            assertFalse(rs.next());

            // Now check for empty match
            query = String.format(queryTemplate, "Windsors");
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSimpleBsonQuery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName() + "bson";
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol bson)";
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setString(3, JsonDoc1);
            stmt.execute();
            conn.commit();
            //TestUtil.dumpTable(conn, TableName.valueOf(tableName));
            String queryTemplate ="SELECT BSON_VALUE(jsoncol, '$.type'), BSON_VALUE(jsoncol, '$.info.address.town'), " +
                "BSON_VALUE(jsoncol, '$.info.tags[1]'), BSON_VALUE(jsoncol, '$.info.tags'), BSON_VALUE(jsoncol, '$.info') " +
                " FROM " + tableName +
                " WHERE BSON_VALUE(jsoncol, '$.name') = '%s'";
            String query = String.format(queryTemplate, "AndersenFamily");
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("Basic", rs.getString(1));
            assertEquals("Bristol", rs.getString(2));
            assertEquals("Water polo", rs.getString(3));
            // returned format is different
            //assertEquals("[\"Sport\",\"Water polo\"]", rs.getString(4));
            //assertEquals("{\"address\":{\"county\":\"Avon\",\"town\":\"Bristol\",\"country\":\"England\"},\"type\":1,\"tags\":[\"Sport\",\"Water polo\"]}", rs.getString(5));
            assertFalse(rs.next());

            // Now check for empty match
            query = String.format(queryTemplate, "Windsors");
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
            fail();
        }
    }

    @Test
    public void testSimpleBsonModify() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol bson)";
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setString(3, JsonDoc1);
            stmt.execute();
            conn.commit();

            String upsert ="UPSERT INTO " + tableName + " VALUES(1,2, BSON_MODIFY(jsoncol, '$.info.address.town', '\"Manchester\"')) ";
            conn.createStatement().execute(upsert);
            TestUtil.dumpTable(conn, TableName.valueOf(tableName));
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1,2, BSON_MODIFY(jsoncol, '$.info.tags[1]', '\"alto1\"')) ");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1,2, BSON_MODIFY(jsoncol, '$.info.tags', '[\"Sport\", \"alto1\", \"Books\"]')) ");
            TestUtil.dumpTable(conn, TableName.valueOf(tableName));

            String queryTemplate ="SELECT BSON_VALUE(jsoncol, '$.type'), BSON_VALUE(jsoncol, '$.info.address.town'), " +
                "BSON_VALUE(jsoncol, '$.info.tags[1]'), BSON_VALUE(jsoncol, '$.info.tags'), BSON_VALUE(jsoncol, '$.info') " +
                " FROM " + tableName +
                " WHERE BSON_VALUE(jsoncol, '$.name') = '%s'";
            String query = String.format(queryTemplate, "AndersenFamily");
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("Basic", rs.getString(1));
            assertEquals("Manchester", rs.getString(2));
            assertEquals("alto1", rs.getString(3));
            //assertEquals("[\"Sport\",\"Water polo\",\"Books\"]", rs.getString(4));
            //assertEquals("{\"type\":1,\"address\":{\"town\":\"Manchester\",\"county\":\"Avon\",\"country\":\"England\"},\"tags\":[\"Sport\",\"Water polo\",\"Books\"]}", rs.getString(5));
            //assertFalse(rs.next());

            // Now check for empty match
            query = String.format(queryTemplate, "Windsors");
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSimpleBsonQuery2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            //String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol.v varchar)";
            String ddl = "create table if not exists " + tableName + " (pk integer primary key, col integer, jsoncol.jsoncol bson) IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES=0";
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setString(3, JsonDoc2);
            stmt.execute();
            conn.commit();
            SingleCellIndexIT.dumpTable(tableName);
            SingleCellIndexIT.dumpTable("SYSTEM.CATALOG");
            ResultSet rs = conn.createStatement().executeQuery("SELECT BSON_VALUE(JSONCOL,'$.test'), BSON_VALUE(JSONCOL, '$.testCnt'), BSON_VALUE(JSONCOL, '$.infoTop[5].info.address.state'),BSON_VALUE(JSONCOL, '$.infoTop[4].tags[1]')  FROM "
                + tableName + " WHERE BSON_VALUE(JSONCOL, '$.test')='test1'");
            assertTrue(rs.next());
            assertEquals("test1", rs.getString(1));
            assertEquals("SomeCnt1", rs.getString(2));
            fail();
        }
    }

    //@Test
    public void testSimpleJsonQueryDCworks() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol.v varchar)";
            //String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol.jsoncol jsondc)";
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("UPSERT INTO " + tableName + " (pk, col, jsoncol.type varchar, jsoncol.name varchar) VALUES (1,2, 'Basic', 'AndersenFamily')"); //+ JsonDoc1 + "')");
            //conn.createStatement().execute("UPSERT INTO " + tableName + " (pk, col, jsoncol) VALUES (1,2, '" + JsonDoc1 + "')");

            SingleCellIndexIT.dumpTable(tableName);
//            String queryTemplate ="SELECT JSON_VALUE_DC(jsoncol, '$.type'), JSON_VALUE_DC(jsoncol, '$.info.address.town'), " +
//                    "JSON_VALUE_DC(jsoncol, '$.info.tags[1]')" +
//                    " FROM " + tableName +
//                    " WHERE JSON_VALUE_DC(jsoncol, '$.name') = '%s'";
            String queryTemplate ="SELECT JSONCOL.type "+//JSON_VALUE_DC(jsoncol, '$.info.address.town') " +
                    //"JSON_VALUE_DC(jsoncol, '$.info.tags[1]')" +
                    " FROM " + tableName +
                    " (JSONCOL.type varchar, JSONCOL.name varchar) WHERE jsoncol.name = '%s'";

            String query = String.format(queryTemplate, "AndersenFamily");

            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("Basic", rs.getString(1));
            assertEquals("Bristol", rs.getString(2));
            assertEquals("Water polo", rs.getString(3));
            assertFalse(rs.next());

            // Now check for empty match
            query = String.format(queryTemplate, "Windsors");
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
        }
    }


    //@Test
    // Test the case where the table DDL contains 1 primary key column and other columns as well
    public void testSinglePkAndOtherColsWithDynamicColumns() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(true);
        String tableName = generateUniqueName();

        StringBuilder sb = new StringBuilder("CREATE ");
        sb.append("TABLE ").append(tableName).append(" (A INTEGER PRIMARY KEY, B VARCHAR) ");
        conn.createStatement().execute(sb.toString());
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (A, DYN1 INTEGER, DYN2 VARCHAR) VALUES(100, 5, 'test3')");

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        while(rs.next()) {
            ResultSetMetaData rmd = rs.getMetaData();
            int count = rmd.getColumnCount();
            for (int i = 1; i <= count; i++) {
                    if (i == 1) {
                        assertEquals("A", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(100, rs.getObject(i));
                    } else if (i == 2) {
                        assertEquals("B", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        // Note that we didn't upsert any value for column 'B' so we should get null
                        assertEquals(null, rs.getObject(i));
                    } else if (i == 3) {
                        assertEquals("DYN1", rmd.getColumnName(i));
                        assertEquals(INTEGER, rmd.getColumnType(i));
                        assertEquals(5, rs.getObject(i));
                    } else if (i == 4) {
                        assertEquals("DYN2", rmd.getColumnName(i));
                        assertEquals(VARCHAR, rmd.getColumnType(i));
                        assertEquals("test3", rs.getObject(i));
                    }
            }
        }
    }
}
