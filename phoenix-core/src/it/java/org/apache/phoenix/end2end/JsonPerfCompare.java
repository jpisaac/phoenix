import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JsonPerfCompare {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonPerfCompare.class);

    private static final Option HELP_OPTION = new Option("h", "help", false, "Help");
    private static final Option RUN_ALL = new Option("a", "run-all", false,
            "Run all scenarios");

    private static final Option RUN_SCENARIO = new Option("s", "run-scenario", true,
            "Run a scenario");

    private static final Option DROP_TABLE = new Option("drop", "drop-table", false,
            "Drop table");

    private String JsonDoc1="{\n" +
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

    private String JsonDoc2="{\n" +
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


    private Configuration conf;
    private boolean runAll;
    private String scenario;
    private int totalRowCount = 8000000;
    private int maxPkValue = 5000000;
    private boolean dropTable;
    public JsonPerfCompare(boolean runAllA, String scenarioA, boolean dropTableA) {
        this.conf = HBaseConfiguration.create();
        this.runAll = runAllA;
        this.scenario = scenarioA;
        this.dropTable = dropTableA;
    }

    private void run() {
        try(Connection conn = ConnectionUtil.getInputConnection(conf)) {
            conn.setAutoCommit(true);
            PhoenixConnection pconn  = conn.unwrap(PhoenixConnection.class);
            createTable(pconn, scenario, dropTable);
            upsert(pconn, scenario);

        } catch (Exception e) {
            LOGGER.error("Error while running Json perf, exiting", e);
            System.exit(-1);
        }
    }

    private void createTable(PhoenixConnection conn, String scenario, boolean dropTable) throws SQLException {
        if (scenario != null) {
            createTableInternal(conn, scenario, dropTable);
            return;
        }

        createTableInternal(conn, "STRING", dropTable);
        createTableInternal(conn, "DC", dropTable);
        createTableInternal(conn, "BINARY", dropTable);
    }

    private void createTableInternal(PhoenixConnection conn, String scenario, boolean dropTable) throws SQLException {
        try {
            if (dropTable) {
                if ("DC".equals(scenario)) {
                    conn.createStatement().execute("DROP TABLE TBL_JSON_DC");
                } else if ("BINARY".equals(scenario)) {
                    conn.createStatement().execute("DROP TABLE TBL_JSON_BINARY");
                } else {
                    conn.createStatement().execute("DROP TABLE TBL_JSON_STRING");
                }
            }
        } catch (Exception e) {
            LOGGER.warn("drop table " + e);
        }

        String ddl = "create table if not exists TBL_JSON_STRING (pk integer primary key, col integer, jsoncol json)";
        if ("DC".equals(scenario)) {
            ddl = "create table if not exists TBL_JSON_DC (pk integer primary key, col integer, jsoncol.jsoncol jsondc)";
        } else if ("BINARY".equals(scenario)) {
            ddl = "create table if not exists TBL_JSON_BINARY (pk integer primary key, col integer, jsoncol.jsoncol jsonb)";
        }
       conn.createStatement().execute(ddl);
    }

    private void upsert(PhoenixConnection conn, String scenario) throws SQLException {
        if (scenario != null) {
            upsertScenario(conn, scenario);
            readScenario(conn, scenario);
            return;
        }

        upsertScenario(conn, "STRING");
        readScenario(conn, "STRING");
        upsertScenario(conn, "DC");
        readScenario(conn, "DC");
        upsertScenario(conn, "BINARY");
        readScenario(conn, "BINARY");
    }

    private void upsertScenario(PhoenixConnection conn, String scenario) throws SQLException {
        String tableName = "TBL_JSON_STRING";
        if ("DC".equals(scenario)) {
            tableName = "TBL_JSON_DC";
        } else if ("BINARY".equals(scenario)) {
            tableName = "TBL_JSON_BINARY";
        }
        String ddl = "UPSERT INTO " + tableName + " (pk, col, jsoncol) VALUES (?,?,?)";

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
            LOGGER.info("Writer (" + scenario
                    + ") committed Batch. Total 1 "
                    + " rows for this thread (" + this.hashCode() + ") in ("
                    + duration + ") Ms");
        }
    }

    private void readScenario(PhoenixConnection conn, String scenario) throws SQLException {
        String ddl ="SELECT JSON_VALUE(JSONCOL,'$.test'), JSON_VALUE(JSONCOL, '$.testCnt'), JSON_VALUE(JSONCOL, '$.infoTop[5].info.address.state'),JSON_VALUE(JSONCOL, '$.infoTop[4].tags[1]'), JSON_VALUE(JSONCOL, '$.infoTop')  FROM "
                + " TBL_JSON_STRING WHERE JSON_VALUE(JSONCOL, '$.test')='test1'";;
        if ("DC".equals(scenario)) {
            ddl ="SELECT JSON_VALUE_DC(JSONCOL,'$.test'), JSON_VALUE_DC(JSONCOL, '$.testCnt'), JSON_VALUE_DC(JSONCOL, '$.infoTop[5].info.address.state'),JSON_VALUE_DC(JSONCOL, '$.infoTop[4].tags[1]'), JSON_VALUE_DC(JSONCOL, '$.infoTop')  FROM "
                    + " TBL_JSON_DC WHERE JSON_VALUE_DC(JSONCOL, '$.test')='test1'";
        } else if ("BINARY".equals(scenario)) {
            ddl = "SELECT JSON_VALUE_B(JSONCOL,'$.test'), JSON_VALUE_B(JSONCOL, '$.testCnt'), JSON_VALUE_B(JSONCOL, '$.infoTop[5].info.address.state'),JSON_VALUE_B(JSONCOL, '$.infoTop[4].tags[1]'), JSON_VALUE_B(JSONCOL, '$.infoTop')  FROM "
             + " TBL_JSON_BINARY WHERE JSON_VALUE_B(JSONCOL, '$.test')='test1'";
        }
        long last = EnvironmentEdgeManager.currentTimeMillis();
        ResultSet rs = conn.createStatement().executeQuery(ddl);
        assertTrue(rs.next());
        assertEquals("test1", rs.getString(1));
        assertEquals("SomeCnt1", rs.getString(2));
        long duration = EnvironmentEdgeManager.currentTimeMillis() - last;
        LOGGER.info("Reader (" + scenario
                + ") " + this.hashCode() + ") in ("
                + duration + ") Ms");

    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption(RUN_ALL);
        options.addOption(RUN_SCENARIO);
        options.addOption(DROP_TABLE);
        options.addOption(HELP_OPTION);
        return options;
    }

    private static void printHelpAndExit(String errorMessage, Options options) {
        System.err.println(errorMessage);
        printHelpAndExit(options, 1);
    }

    private static void printHelpAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        System.exit(exitCode);
    }

    private static CommandLine parseOptions(String[] args) {
        final Options options = getOptions();
        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("Error parsing command line options: " + e.getMessage(), options);
        }

        if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
            printHelpAndExit(options, 0);
        }
        return cmdLine;
    }

    public static void main(String[] args) {
        LOGGER.info("Json scenario testing");

        try {
            CommandLine cmdLine = parseOptions(args);
            boolean runAll = false;
            String scenario = null;
            boolean dropTable = false;
            if (cmdLine.hasOption(RUN_ALL.getOpt())) {
                runAll = true;
            } else {
                String value = cmdLine.getOptionValue(RUN_SCENARIO.getOpt());
                scenario = value;
            }
            if (cmdLine.hasOption(DROP_TABLE.getOpt())) {
                dropTable = true;
            }
            JsonPerfCompare rm = new JsonPerfCompare(runAll, scenario, dropTable);
            rm.run();
        } catch (IllegalStateException e) {
            printHelpAndExit(e.getMessage(), getOptions());
        }
    }

}

