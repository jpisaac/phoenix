package org.apache.phoenix.pherf.configuration;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Map;

@XmlRootElement(name = "workload-profile")
public class WorkloadProfile {
    private String name;
    private Map<String, String> scenarioProperties;
    private LoadProfile loadProfile;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getScenarioProperties() {
        return scenarioProperties;
    }

    public void setScenarioProperties(Map<String, String> scenarioProperties) {
        this.scenarioProperties = scenarioProperties;
    }

    public LoadProfile getLoadProfile() {
        return loadProfile;
    }

    public void setLoadProfile(LoadProfile loadProfile) {
        this.loadProfile = loadProfile;
    }
}


