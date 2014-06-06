
package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Thread used to receive messages from another AVD's client thread. This thread
 * is always running in the background and is listening on its socket for an
 * incoming request and keeps blocking using the accept method until it receives
 * any data. The message types join and joinreply for node joining and ring
 * forming are handled here
 * 
 * @author Vivek Bhalla
 */
public class ServerThread implements Runnable {
    static final int SERVER_PORT = 10000; // Port at which the server listens

    @Override
    public void run() {

        Socket socket = null;
        try {
            SimpleDynamoProvider.serverSocket = new ServerSocket(SERVER_PORT);
            Log.v("ServerThread", "SocketCreated");
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        try {

            while (true) {

                // Accept Connection and Initialize Input Stream
                socket = SimpleDynamoProvider.serverSocket.accept();

                Identifier ident = new Identifier(socket);
                Thread identThread = new Thread(ident);
                identThread.start();

            }

        } catch (EOFException e) {
            Log.e("ServerThread", "EOFException");
            e.printStackTrace();
        } catch (IOException e) {
            Log.e("ServerThread", "IOException");
            e.printStackTrace();
        } catch (Exception e) {
            Log.e("ServerThread", "Exception");
            e.printStackTrace();
            // new ServerThread()
        }

    }
}
