package org.example;

import java.net.UnknownHostException;

public class NebulaHelper {

    private static NebulaClient nebulaSingletonClient;

    public static NebulaClient getNebulaClient(String host, int port) throws UnknownHostException {
        return new NebulaClient(host, port);
    }

    public static NebulaClient getNebulaSingletonClient(String host, int port) throws UnknownHostException {
        if (nebulaSingletonClient == null) {
            nebulaSingletonClient = new NebulaClient(host, port);
        }
        return nebulaSingletonClient;
    }
}
