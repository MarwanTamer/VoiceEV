package com.orange.marwan.VoiceEventConsumer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ResourceBundle;

public class ConstantUtls {
    static String bootStraperServer;
    static String consumerTopicName;
    static String producerTopicName;
    static ResourceBundle resourceBundle;
    static ObjectMapper mapper;
    public static void loadCons() {
        resourceBundle = ResourceBundle.getBundle("application");
        bootStraperServer = resourceBundle.getString("bootStServer");
        consumerTopicName  = resourceBundle.getString("c_topic_nme");
        producerTopicName = resourceBundle.getString("p_topic_nme");
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
    }
}
