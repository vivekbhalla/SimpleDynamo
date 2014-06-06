
package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.MatrixCursor;
import android.util.Log;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

/**
 * Thread used to send messages from the AVD to another AVD's server.
 * 
 * @author Vivek Bhalla
 */
public class ClientThread implements Runnable {
    NodeMessage msg;
    NodeMessage recmsg;
    private static final String COLUMN_1 = "key";
    private static final String COLUMN_2 = "value";
    private static final String[] columnNames = {
            COLUMN_1, COLUMN_2
    };

    /*
     * Constructor to accept the NodeMessage object.
     */
    public ClientThread(NodeMessage msg) {
        this.msg = msg;
    }

    @Override
    public void run() {
        Socket socket = null;
        ObjectOutputStream output = null;
        ObjectInputStream input = null;
        try {
            // Create Socket
            socket = new Socket();
            socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(msg.sendPort)), 500);

            // Initialize Output Stream
            output = new ObjectOutputStream(new BufferedOutputStream(
                    socket.getOutputStream()));

            // Write the NodeMessage Object
            output.writeObject(msg);
            output.flush();

            // Initialize Input Stream
            input = new ObjectInputStream(new BufferedInputStream(
                    socket.getInputStream()));

            recmsg = (NodeMessage) input.readObject();

            socket.close();

            replyHandler();

        } catch (SocketTimeoutException e) {
            Log.e("ClientThread", "SocketTimeoutException");
            e.printStackTrace();
            // Node Failure
            failureHandler();
        } catch (EOFException e) {
            Log.e("ClientThread", "EOFException");
            e.printStackTrace();
            // Node Failure
            failureHandler();
        } catch (SocketException e) {
            Log.e("ClientThread", "EOFException");
            e.printStackTrace();
            // Node Failure
            failureHandler();
        } catch (Exception e) {
            Log.e("ClientThread", "Exception");
            e.printStackTrace();
        }
    }

    /* Method to handle different types of reply */
    private void replyHandler() {

        if (recmsg.type.equals("insertreply")) {

            Log.v("CT- Insert Reply from ", recmsg.myPort + ", " + recmsg.selection);

        } else if (recmsg.type.equals("replicate1reply")) {

            Log.v("CT- Replicate1 Reply from ", recmsg.myPort + ", " + recmsg.selection);

        } else if (recmsg.type.equals("replicate2reply")) {

            Log.v("CT- Replicate2 Reply from ", recmsg.myPort + ", " + recmsg.selection);

        } else if (recmsg.type.equals("queryreply")) {

            queryReply();

        } else if (recmsg.type.equals("globalqueryforwarded")
                || recmsg.type.equals("globalquerydone")) {

            Log.v("CT- " + recmsg.type + " from ", recmsg.myPort);

        } else if (recmsg.type.equals("deletereply")) {

            Log.v("CT- Delete Reply from ", recmsg.myPort);

        } else if (recmsg.type.equals("globaldeletereply")) {

            Log.v("CT- Global Delete Reply from ", recmsg.myPort);

        } else if (recmsg.type.equals("recoverreply")) {

            Log.v("CT- Recover Reply from ", recmsg.myPort);
            // SimpleDynamoProvider.waitOver = true;
            new Thread(new Recovery(recmsg)).start();

        } else if (recmsg.type.equals("error")) {

            Log.e("CT- Error from Identifier ", recmsg.myPort);

        }

    }

    /* Method to handle different types of failure */
    private void failureHandler() {

        String[] successors = SimpleDynamoProvider.getSuc(msg.sendPort);

        if (msg.type.equals("insert")) {

            Log.v("CT- Send Insert Failed to " + msg.sendPort, msg.cv[0] + " ,"
                    + msg.cv[1]);

            sendMsg(msg, "replicate1", successors[0]);

        } else if (msg.type.equals("replicate1")) {

            Log.v("CT- Send Replicate1 Failed to " + msg.sendPort, msg.cv[0] + " ,"
                    + msg.cv[1]);

            sendMsg(msg, "replicate2", successors[0]);

        } else if (msg.type.equals("query")) {

            Log.v("CT- Send Query Failed to " + msg.sendPort, msg.selection);
            sendQuery(successors[0], msg);

        } else if (msg.type.equals("globalquery")) {

            Log.v("CT- Send GlobaQuery Failed to " + msg.sendPort, "Map Size "
                    + msg.globalCursor.size());
            sendGlobalQuery(successors[0], msg);

        } else if (msg.type.equals("delete")) {

            Log.v("CT- Send Delete Failed to " + msg.sendPort, msg.selection);
            sendDelete("deletereplica1", successors[0], msg);

        } else if (msg.type.equals("deletereplica1")) {

            Log.v("CT- Send Delete Replica 1 Failed to " + msg.sendPort, msg.selection);
            sendDelete("deletereplica2", successors[0], msg);

        } else if (msg.type.equals("deleteglobal")) {

            Log.v("CT- Failure type - " + msg.type + "doesn't need handling.", "");

        } else {

            Log.v("CT- Failure type - " + msg.type + "doesn't need handling.", "");

        }
    }

    /* Get Query Reply on the same socket and get out of Busy-Wait Query loop */
    public void queryReply() {

        Log.v("CT- queryReply from " + recmsg.myPort, recmsg.cv[0] + ", " + recmsg.cv[1]);

        MatrixCursor cursor = new MatrixCursor(columnNames);

        cursor.addRow(recmsg.cv);

        SimpleDynamoProvider.cursor = cursor;
        SimpleDynamoProvider.value = recmsg.cv[1];
        SimpleDynamoProvider.waitOver = true;
        Log.v("CT- queryReply", "Done");
    }

    /* Replicate to successors on failure */
    public void sendMsg(NodeMessage insertMsg, String type, String port) {

        insertMsg.type = type;
        insertMsg.sendPort = port;

        ClientThread insert = new ClientThread(insertMsg);
        Thread t = new Thread(insert);
        t.start();

        Log.v("CT- sendMsg " + type + " to " + port, insertMsg.cv[0] + ", "
                + insertMsg.cv[1]);

    }

    /* Send Query request to successor on failure */
    private void sendQuery(String successor, NodeMessage queryMsg) {

        queryMsg.sendPort = successor;

        ClientThread query = new ClientThread(queryMsg);
        Thread t = new Thread(query);
        t.start();

        Log.v("CT- sendQuery " + " to " + successor, msg.selection);

    }

    /* Send Delete request to successor on failure */
    private void sendDelete(String type, String suc, NodeMessage deletemsg) {

        deletemsg.type = type;
        deletemsg.sendPort = suc;

        ClientThread delete = new ClientThread(deletemsg);
        Thread t = new Thread(delete);
        t.start();

        Log.v("CT- " + type + " to " + suc, msg.selection);
    }

    /* Send Global Query request to successor on failure */
    private void sendGlobalQuery(String successor, NodeMessage globalQuery) {

        globalQuery.sendPort = successor;
        System.out.println("Global Query Map Size " + globalQuery.globalCursor.size());
        System.out.println("Global Query Query Port " + globalQuery.queryPort);
        ClientThread global = new ClientThread(globalQuery);
        Thread t = new Thread(global);
        t.start();

        Log.v("CT- Send GlobalQuery to " + successor, "Map Size "
                + globalQuery.globalCursor.size());

    }

}
