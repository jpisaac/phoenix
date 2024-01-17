/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf.configuration;

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.exception.FileLoaderException;
import org.apache.phoenix.pherf.util.ResourceList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ScenarioProfileParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScenarioProfileParser.class);
    private String filePattern;
    private LoadProfile loadProfile;
    private ResourceList resourceList;
    private Collection<Path> paths = null;

    public ScenarioProfileParser(String pattern) throws Exception {
        init(pattern);
    }

    public LoadProfile getLoadProfile() {
        return loadProfile;
    }

    public synchronized Collection<Path> getPaths(String strPattern) throws Exception {
        if (paths != null) {
            return paths;
        }
        paths = getResources(strPattern);
        return paths;
    }


    public String getFilePattern() {
        return filePattern;
    }

    /**
     * Unmarshall an XML data file
     *
     * @param file Name of File
     * @return {@link DataModel} Returns DataModel from
     * XML configuration
     */
    // TODO Remove static calls
    public static DataModel readDataModel(Path file) throws JAXBException, XMLStreamException {
        XMLInputFactory xif = XMLInputFactory.newInstance();
        xif.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        JAXBContext jaxbContext = JAXBContext.newInstance(DataModel.class);
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
        String fName = PherfConstants.RESOURCE_SCENARIO + "/" + file.getFileName().toString();
        LOGGER.info("Open config file: " + fName);
        XMLStreamReader xmlReader = xif.createXMLStreamReader(
            new StreamSource(ScenarioProfileParser.class.getResourceAsStream(fName)));
        return (DataModel) jaxbUnmarshaller.unmarshal(xmlReader);
    }

    // TODO Remove static calls
    public static String parseSchemaName(String fullTableName) {
        String ret = null;
        if (fullTableName.contains(".")) {
            ret = fullTableName.substring(0, fullTableName.indexOf("."));
        }
        return ret;
    }

    // TODO Remove static calls
    public static String parseTableName(String fullTableName) {
        String ret = fullTableName;
        if (fullTableName.contains(".")) {
            ret = fullTableName.substring(fullTableName.indexOf(".") + 1, fullTableName.length());
        }
        // Remove any quotes that may be needed for multi-tenant tables
        ret = ret.replaceAll("\"", "");
        return ret;
    }

    // TODO Remove static calls
    @SuppressWarnings("unused")
    public static void writeDataModel(DataModel data, OutputStream output) throws JAXBException {
        // create JAXB context and initializing Marshaller
        JAXBContext jaxbContext = JAXBContext.newInstance(DataModel.class);
        Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

        // for getting nice formatted output
        jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

        // Writing to console
        jaxbMarshaller.marshal(data, output);
    }

    private void init(String pattern) throws Exception {
        if (loadProfile != null) {
            return;
        }
        this.filePattern = pattern;
        //this.dataModels = new ArrayList<>();
        this.resourceList = new ResourceList(PherfConstants.RESOURCE_SCENARIO);
        this.paths = getResources(this.filePattern);
        if (this.paths.isEmpty()) {
            throw new FileLoaderException(
                    "Could not load the resource files using the pattern: " + pattern);
        }
        for (Path path : this.paths) {
            System.out.println("Adding model for path:" + path.toString());
          //  this.dataModels.add(ScenarioProfileParser.readDataModel(path));
        }
    }

    private Collection<Path> getResources(String pattern) throws Exception {
        return resourceList.getResourceList(pattern);
    }
}
