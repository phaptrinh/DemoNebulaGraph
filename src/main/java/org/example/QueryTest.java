package org.example;

import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.Session;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.List;

public class QueryTest {
    public static void main(String[] args) throws IOErrorException, AuthFailedException, ClientServerIncompatibleException, NotValidConnectionException, UnknownHostException, UnsupportedEncodingException {

        NebulaClient nebulaClient = NebulaHelper.getNebulaClient(Config.NEBULA_HOST, Config.NEBULA_PORT);
        NebulaClient nebulaClient2 = NebulaHelper.getNebulaClient(Config.NEBULA_HOST, Config.NEBULA_PORT);

        NebulaClient nebulaClient3 = NebulaHelper.getNebulaSingletonClient(Config.NEBULA_HOST, Config.NEBULA_PORT);
        NebulaClient nebulaClient4 = NebulaHelper.getNebulaSingletonClient(Config.NEBULA_HOST, Config.NEBULA_PORT);

//        System.out.println(nebulaClient == nebulaClient2);
//        System.out.println(nebulaClient3 == nebulaClient4);


        Session session = nebulaClient3.getSession(Config.USERNAME, Config.PASSWORD, Config.RECONNECT);
        Session session2 = nebulaClient4.getSession(Config.USERNAME, Config.PASSWORD, Config.RECONNECT);

        ResultSet resultSet = session.execute("SHOW HOSTS;");
        printResult(resultSet);
        session.release();
        session2.release();
        nebulaClient3.close();
        nebulaClient4.close();


    }

    private static void printResult(ResultSet resultSet) throws UnsupportedEncodingException {
        List<String> colNames = resultSet.keys();
        for (String name : colNames) {
            System.out.printf("%15s |", name);
        }
        System.out.println();
        for (int i = 0; i < resultSet.rowsSize(); i++) {
            ResultSet.Record record = resultSet.rowValues(i);
            for (ValueWrapper value : record.values()) {
                if (value.isLong()) {
                    System.out.printf("%15s |", value.asLong());
                }
                if (value.isBoolean()) {
                    System.out.printf("%15s |", value.asBoolean());
                }
                if (value.isDouble()) {
                    System.out.printf("%15s |", value.asDouble());
                }
                if (value.isString()) {
                    System.out.printf("%15s |", value.asString());
                }
                if (value.isTime()) {
                    System.out.printf("%15s |", value.asTime());
                }
                if (value.isDate()) {
                    System.out.printf("%15s |", value.asDate());
                }
                if (value.isDateTime()) {
                    System.out.printf("%15s |", value.asDateTime());
                }
                if (value.isVertex()) {
                    System.out.printf("%15s |", value.asNode());
                }
                if (value.isEdge()) {
                    System.out.printf("%15s |", value.asRelationship());
                }
                if (value.isPath()) {
                    System.out.printf("%15s |", value.asPath());
                }
                if (value.isList()) {
                    System.out.printf("%15s |", value.asList());
                }
                if (value.isSet()) {
                    System.out.printf("%15s |", value.asSet());
                }
                if (value.isMap()) {
                    System.out.printf("%15s |", value.asMap());
                }
            }
            System.out.println();
        }
    }

}