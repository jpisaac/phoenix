/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.pherf.util;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.pherf.exception.PherfException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * list resources available from the classpath @ *
 */
public class ResourceList {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceList.class);
  private final String resourceType; // e.g., "scenario", "datamodel", "profile"
  private final String rootResourceDir;
  // Lists the directories to ignore meant for testing something else
  private List<String> dirsToIgnore = new ArrayList<>();

  public ResourceList(String resourceType) {
    this(resourceType, "");
  }

  public ResourceList(String resourceType, String rootResourceDir) {
    // Remove leading slash from resourceType if present
    if (resourceType.startsWith("/")) {
      resourceType = resourceType.substring(1);
    }
    this.resourceType = resourceType;
    
    // Handle rootResourceDir - remove leading slash and ensure trailing slash
    if (rootResourceDir.startsWith("/")) {
      rootResourceDir = rootResourceDir.substring(1);
    }
    this.rootResourceDir = rootResourceDir.isEmpty() ? "" : 
                          (rootResourceDir.endsWith("/") ? rootResourceDir : rootResourceDir + "/");
  }

  public Collection<Path> getResourceList(final String pattern) throws Exception {
    Collection<Path> paths = new ArrayList<>();
    Pattern compiledPattern = Pattern.compile(pattern);
    
    // Get the class loader
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = ResourceList.class.getClassLoader();
    }
    
    // Look for resources of the specified type
    String resourcePath = this.rootResourceDir + this.resourceType;
    Enumeration<URL> resources = classLoader.getResources(resourcePath);
    
    while (resources.hasMoreElements()) {
      URL resource = resources.nextElement();
      if ("file".equals(resource.getProtocol())) {
        // Handle file system resources
        processFileSystemResource(resource, compiledPattern, paths);
      } else if ("jar".equals(resource.getProtocol())) {
        // Handle JAR resources
        processJarResource(resource, compiledPattern, paths, classLoader);
      }
    }
    
    return paths;
  }

  private void processFileSystemResource(URL resource, Pattern pattern, Collection<Path> paths) throws Exception {
    try {
      File file = new File(resource.toURI());
      if (file.isDirectory()) {
        Files.walk(file.toPath())
          .filter(Files::isRegularFile)
          .filter(p -> {
            // Try matching against both the filename and the full path
            String fileName = p.getFileName().toString();
            String fullPath = p.toString();
            return pattern.matcher(fileName).find() || pattern.matcher(fullPath).find();
          })
          .forEach(paths::add);
      }
    } catch (Exception e) {
      LOGGER.error("Error processing file system resource: " + resource, e);
      throw e;
    }
  }
  
  private void processJarResource(URL resource, Pattern pattern, 
                                Collection<Path> paths, ClassLoader classLoader) throws Exception {
    try {
      String jarPath = resource.getPath().split("!")[0];
      if (jarPath.startsWith("file:")) {
        jarPath = jarPath.substring(5);
      }
      
      try (JarFile jar = new JarFile(jarPath)) {
        Enumeration<JarEntry> entries = jar.entries();
        while (entries.hasMoreElements()) {
          JarEntry entry = entries.nextElement();
          String entryName = entry.getName();
          if (entryName.startsWith(this.rootResourceDir + this.resourceType) && 
              pattern.matcher(entryName).find()) {
            URL url = classLoader.getResource(entryName);
            if (url != null) {
              try {
                paths.add(Paths.get(url.toURI()));
              } catch (Exception e) {
                LOGGER.warn("Could not convert URL to Path: " + url, e);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error processing JAR resource: " + resource, e);
      throw e;
    }
  }

  public void setDirsToIgnore(List<String> dirs) {
    this.dirsToIgnore = dirs;
  }
}
