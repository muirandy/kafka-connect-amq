package com.aimyourtechnology.kafka.connect.activemq.connector;

import java.io.IOException;
import java.util.Properties;

class AppVersion {

    static String getVersion() {
        try {
            return readVersionFromPropertiesFile();
        } catch (IOException e) {
            throw new VersionDoesNotExistException(e);
        }
    }

    private static String readVersionFromPropertiesFile() throws IOException {
        Properties props = new Properties();
        props.load(AppVersion.class.getResourceAsStream("/kafka-connect-amq-version.properties"));
        return  props.getProperty("version").trim();
    }

    private static class VersionDoesNotExistException extends RuntimeException {
        VersionDoesNotExistException(IOException e) {
            super(e);
        }
    }
}
