package org.gbif.pipelines.ws.config;

/**
 * Enum of web services supported in the application.
 */
public enum Service {
  SPECIES_MATCH2("match2"), GEO_CODE("geocode");

  // path to use in the properties file
  private String path;

  Service(String path) {
    this.path = path;
  }

  public String getPath() {
    return this.path;
  }

}