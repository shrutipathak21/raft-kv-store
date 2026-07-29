package com.raftkv.rpc;

import com.raftkv.util.NodeConfig;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public final class RpcClient {

    private RpcClient() {
    }

    public static Object send(NodeConfig target, Object request, int timeoutMs) throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.socket().connect(new InetSocketAddress(target.host(), target.raftPort()), timeoutMs);
            channel.socket().setSoTimeout(timeoutMs);
            channel.socket().setTcpNoDelay(true);

            ObjectOutputStream out = new ObjectOutputStream(channel.socket().getOutputStream());
            out.writeObject(request);
            out.flush();

            ObjectInputStream in = new ObjectInputStream(channel.socket().getInputStream());
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Malformed RPC response from " + target.raftAddress(), e);
        }
    }
}
