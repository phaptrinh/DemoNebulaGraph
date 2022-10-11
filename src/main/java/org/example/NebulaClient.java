package org.example;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class NebulaClient {
    private static final Logger log = LoggerFactory.getLogger(NebulaClient.class);

    private final NebulaPool nebulaPool;

    public NebulaClient(String host, int port) throws UnknownHostException {
        String[] addresses = host.split(",");
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        nebulaPoolConfig.setMaxConnSize(2000);
//        nebulaPoolConfig.setTimeout(3);
        List<HostAddress> addressList = new ArrayList<>();
        for (String address: addresses) {
            addressList.add(new HostAddress(address, port));
        }
        NebulaPool pool = new NebulaPool();
        pool.init(addressList, nebulaPoolConfig);
        nebulaPool = pool;
    }

    public Session getSession(String userName, String password, boolean reconnect) throws IOErrorException, AuthFailedException, ClientServerIncompatibleException, NotValidConnectionException {
//        System.out.println(nebulaPool);
        return nebulaPool.getSession(userName, password, reconnect);
    }

    public void close() {
        nebulaPool.close();
    }
}
