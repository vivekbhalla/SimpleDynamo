
package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.MatrixCursor;
import android.util.Log;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.Socket;
import java.util.HashMap;

/**
 * Class to Identify the type of message in the NodeMessage object and perform
 * actions accordingly. The various types of messages are - insert, query,
 * queryreply, globalquery, delete, globaldelete. The message types - join and
 * joinreply are handled in the server thread itself.
 * 
 * @author Vivek Bhalla
 */
public class Identifier implements Runnable {
    public static Context context;
    private static final String COLUMN_1 = "key";
    private static final String COLUMN_2 = "value";
    private static final String[] columnNames = {
            COLUMN_1, COLUMN_2
    };
    private NodeMessage msg;
    private Socket socket;

    /*
     * Constructor accepts the context for the ContentProvider passed from the
     * Server Thread.
     */
    public Identifier(Context context) {
        Identifier.context = context;
    }

    public Identifier(Socket socket) {

        this.socket = socket;

        try {
            ObjectInputStream input = new ObjectInputStream(new BufferedInputStream(
                    socket.getInputStream()));
            this.msg = (NodeMessage) input.readObject();
        } catch (StreamCorruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void run() {
        try {
            NodeMessage reply = process();

            ObjectOutputStream output = null;
            // Initialize Output Stream
            try {
                output = new ObjectOutputStream(new BufferedOutputStream(
                        this.socket.getOutputStream()));
                output.writeObject(reply);
                output.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * Method to actually process the message type request.
     */
    public NodeMessage process() {

        if (msg.type.equals("insert")) {

            return insert();

        } else if (msg.type.equals("replicate1")) {

            return replicate1();

        } else if (msg.type.equals("replicate2")) {

            return replicate2();

        } else if (msg.type.equals("query")) {

            return localQuery();

        } else if (msg.type.equals("globalquery")) {

            return globalQuery();

        } else if (msg.type.equals("delete")) {

            return delete();

        } else if (msg.type.equals("deletereplica1")) {

            return delReplica1();

        } else if (msg.type.equals("deletereplica2")) {

            return delReplica2();

        } else if (msg.type.equals("globaldelete")) {

            return globalDelete();

        } else if (msg.type.equals("recoverself")) {

            return getRecover(msg.type);

        } else if (msg.type.equals("recoverreplica")) {

            return getRecover(msg.type);

        } else {

            Log.e("I", "Invalid Message -" + msg.type);
            NodeMessage error = new NodeMessage();
            error.type = "error";
            error.myPort = SimpleDynamoProvider.myPort;
            return error;
        }

    }

    public NodeMessage insert() {

        Log.v("I-insert from " + msg.myPort, msg.cv[0] + " ," + msg.cv[1]);
        synchronized (this) {
            SimpleDynamoProvider.CV.put(msg.cv[0], msg.cv[1]);
        }
        NodeMessage insertreply = new NodeMessage();
        insertreply.type = "insertreply";
        insertreply.myPort = SimpleDynamoProvider.myPort;
        insertreply.selection = msg.cv[0];

        sendMsg(msg, "replicate1", SimpleDynamoProvider.successor1);

        return insertreply;
    }

    public NodeMessage replicate1() {

        Log.v("I-replicate1 from " + msg.myPort, msg.cv[0] + " ," + msg.cv[1]);
        synchronized (this) {
            SimpleDynamoProvider.CV.put(msg.cv[0], msg.cv[1]);
        }
        NodeMessage replicatereply = new NodeMessage();
        replicatereply.type = "replicate1reply";
        replicatereply.myPort = SimpleDynamoProvider.myPort;
        replicatereply.selection = msg.cv[0];

        sendMsg(msg, "replicate2", SimpleDynamoProvider.successor1);

        return replicatereply;

    }

    public NodeMessage replicate2() {

        Log.v("I-replicate2 from " + msg.myPort, msg.cv[0] + " ," + msg.cv[1]);
        synchronized (this) {
            SimpleDynamoProvider.CV.put(msg.cv[0], msg.cv[1]);
        }
        NodeMessage replicatereply = new NodeMessage();
        replicatereply.type = "replicate2reply";
        replicatereply.myPort = SimpleDynamoProvider.myPort;
        replicatereply.selection = msg.cv[0];

        return replicatereply;

    }

    private NodeMessage localQuery() {

        Log.v("I- localQuery from " + msg.myPort, msg.selection);

        NodeMessage queryReply = new NodeMessage();
        queryReply.type = "queryreply";
        queryReply.myPort = SimpleDynamoProvider.myPort;
        queryReply.sendPort = msg.myPort;
        queryReply.cv = new String[] {
                msg.selection, SimpleDynamoProvider.CV.get(msg.selection)
        };
        Log.v("I- localQuery - queryReply to " + msg.myPort,
                queryReply.cv[0] + ", " + queryReply.cv[1]);

        return queryReply;
    }

    public NodeMessage globalQuery() {

        Log.v("I- globalQuery from " + msg.myPort, "Map Size " + msg.globalCursor.size());

        NodeMessage globalreply = new NodeMessage();
        globalreply.myPort = SimpleDynamoProvider.myPort;

        if (!msg.queryPort.equals(SimpleDynamoProvider.myPort)) {

            getLocalforGlobal();
            globalreply.type = "globalqueryforwarded";

        } else {

            MatrixCursor cursor = new MatrixCursor(columnNames);

            for (String key : msg.globalCursor.keySet()) {
                String[] row = {
                        key, msg.globalCursor.get(key)
                };
                cursor.addRow(row);
            }

            globalreply.type = "globalquerydone";

            SimpleDynamoProvider.cursor = cursor;
            SimpleDynamoProvider.waitOver = true;

        }

        return globalreply;

    }

    public NodeMessage delete() {

        synchronized (this) {
            SimpleDynamoProvider.CV.remove(msg.selection);
        }
        NodeMessage deletereply = new NodeMessage();
        deletereply.type = "deletereply";
        deletereply.myPort = SimpleDynamoProvider.myPort;

        sendDelete("deletereplica1", SimpleDynamoProvider.successor1);

        return deletereply;
    }

    private NodeMessage delReplica1() {
        synchronized (this) {
            SimpleDynamoProvider.CV.remove(msg.selection);
        }
        NodeMessage deletereply = new NodeMessage();
        deletereply.type = "deletereply";
        deletereply.myPort = SimpleDynamoProvider.myPort;

        sendDelete("deletereplica2", SimpleDynamoProvider.successor1);

        return deletereply;

    }

    private NodeMessage delReplica2() {
        synchronized (this) {
            SimpleDynamoProvider.CV.remove(msg.selection);
        }
        NodeMessage deletereply = new NodeMessage();
        deletereply.type = "deletereply";
        deletereply.myPort = SimpleDynamoProvider.myPort;

        return deletereply;
    }

    private void sendDelete(String type, String suc) {

        NodeMessage deletemsg = new NodeMessage();
        deletemsg.type = type;
        deletemsg.myPort = SimpleDynamoProvider.myPort;
        deletemsg.sendPort = suc;
        deletemsg.selection = msg.selection;

        ClientThread delete = new ClientThread(deletemsg);
        Thread t = new Thread(delete);
        t.start();

        Log.v("I- " + type + " to " + suc, msg.selection);
    }

    public NodeMessage globalDelete() {
        synchronized (this) {
            SimpleDynamoProvider.CV.clear();
        }
        NodeMessage globaldeletereply = new NodeMessage();
        globaldeletereply.type = "globaldeletereply";
        globaldeletereply.myPort = SimpleDynamoProvider.myPort;

        return globaldeletereply;

    }

    private NodeMessage getRecover(String type) {

        Log.v("Identifier- getRecover from " + msg.myPort, type);

        NodeMessage recoverReply = new NodeMessage();

        // HashMap<String, String> recoverCursor = new HashMap<String,
        // String>();
        //
        // if (type.equals("recoverself")) {
        // for (String key : SimpleDynamoProvider.CV.keySet()) {
        //
        // if (self(key, msg.myPort, msg.preID)) {
        // Log.v("Identifier- getRecover - Self", key);
        //
        // recoverCursor.put(key, SimpleDynamoProvider.CV.get(key));
        // }
        // }
        //
        // } else if (type.equals("recoverreplica")) {
        // for (String key : SimpleDynamoProvider.CV.keySet()) {
        //
        // if (replica(key)) {
        // Log.v("Identifier- getRecover - Replica", key);
        //
        // recoverCursor.put(key, SimpleDynamoProvider.CV.get(key));
        // }
        // }
        //
        // }

        recoverReply.type = "recoverreply";
        recoverReply.myPort = SimpleDynamoProvider.myPort;
        recoverReply.globalCursor = new HashMap<String, String>();
        recoverReply.globalCursor.putAll(SimpleDynamoProvider.CV);
        recoverReply.largestID = type;
        recoverReply.preID = SimpleDynamoProvider.predecessor1ID;

        Log.v("Identifier- getRecover - RecoverReply", type + " ," + msg.myPort + ", Map Size "
                + recoverReply.globalCursor.size());

        return recoverReply;

    }

    private void getLocalforGlobal() {
        synchronized (this) {
            msg.globalCursor.putAll(SimpleDynamoProvider.CV);
        }
        NodeMessage globalforward = new NodeMessage();
        globalforward.type = "globalquery";
        globalforward.globalCursor = msg.globalCursor;
        globalforward.myPort = SimpleDynamoProvider.myPort;
        globalforward.sendPort = SimpleDynamoProvider.successor1;
        globalforward.queryPort = msg.queryPort;

        System.out
                .println("Global Forward - Map Size " + globalforward.globalCursor.size()
                        + ", Query Port " + globalforward.queryPort + ", Send Port "
                        + globalforward.sendPort);
        ClientThread global = new ClientThread(globalforward);
        Thread t = new Thread(global);
        t.start();

        Log.v("Identifier-getLocalforGlobal - Send global query reply to "
                + msg.myPort,
                "Map Size " + globalforward.globalCursor.size());

    }

    public void sendMsg(NodeMessage insertMsg, String type, String port) {

        insertMsg.type = type;
        insertMsg.myPort = SimpleDynamoProvider.myPort;
        insertMsg.sendPort = port;

        ClientThread insert = new ClientThread(insertMsg);
        Thread t = new Thread(insert);
        t.start();

        Log.v("I- sendMsg " + type + " to " + port, insertMsg.cv[0] + ", " + insertMsg.cv[1]);

    }
}
