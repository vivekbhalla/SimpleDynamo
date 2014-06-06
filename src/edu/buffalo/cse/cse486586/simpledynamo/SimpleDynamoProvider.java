
package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;

public class SimpleDynamoProvider extends ContentProvider {

    public static String myPort; // AVD's port
    public static String myID; // Hashed ID for this AVD
    static Context context; // Context for the content provider
    public static boolean isReplicate = false;
    public static boolean isRestart = false;
    public static boolean isLoading = true;
    public static boolean queryFlag = false; // Flag to indicate that a query
                                             // type message has been received
                                             // from another
                                             // node and a queryreply type of
                                             // message has to be created
    public static boolean insertFlag = false;
    public static String queryPort; // Used to set the port which sent the
                                    // query/delete request
    // public static boolean isReplicate = false;
    public static boolean waitOver = true; // Flag in-order to busy wait the
                                           // main thread, till a reply is
                                           // received in the server thread
    public static Identifier ident; // Identifier class object, used to identify
                                    // the type of query and process it
    private static final String COLUMN_1 = "key";
    private static final String COLUMN_2 = "value";
    private static final String[] columnNames = {
            COLUMN_1, COLUMN_2
    };
    public static MatrixCursor cursor; // MatrixCusor object used to store the
                                       // cursor returned by query
    public static HashMap<String, String> localCursor = new HashMap<String, String>();

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    static final String REMOTE_PORT0ID = "5554";
    static final String REMOTE_PORT1ID = "5556";
    static final String REMOTE_PORT2ID = "5558";
    static final String REMOTE_PORT3ID = "5560";
    static final String REMOTE_PORT4ID = "5562";

    static String[] portGroup;

    static String[] portIDGroup;

    public static String successor1;
    public static String successor2;

    public static String predecessor1;
    public static String predecessor2;

    public static String successor1ID;
    public static String successor2ID;

    public static String predecessor1ID;
    public static String predecessor2ID;

    public static ServerSocket serverSocket;
    public static String value = null;

    public static HashMap<String, String> CV = new HashMap<String, String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        if (selection.equals("*")) {

            return delGlobal();

        } else if (selection.equals("@")) {

            return delLocal();

        } else {

            String partitionPort = getPartitionPort(selection);

            if (partitionPort.equals(myPort)) {

                return localDelete(selection);

            } else {

                sendDelete(selection, partitionPort);
                return 0;

            }
        }

    }

    private int localDelete(String selection) {
        synchronized (this) {
            CV.remove(selection);
        }
        NodeMessage deletemsg = new NodeMessage();
        deletemsg.type = "deletereplica1";
        deletemsg.myPort = myPort;
        deletemsg.sendPort = successor1;
        deletemsg.selection = selection;

        ClientThread delete = new ClientThread(deletemsg);
        Thread t = new Thread(delete);
        t.start();

        Log.v("CP- sendDelete replicate1 to " + successor1, selection);

        return 1;
    }

    private void sendDelete(String selection, String port) {

        NodeMessage deletemsg = new NodeMessage();
        deletemsg.type = "delete";
        deletemsg.myPort = myPort;
        deletemsg.sendPort = port;
        deletemsg.selection = selection;

        ClientThread delete = new ClientThread(deletemsg);
        Thread t = new Thread(delete);
        t.start();

        Log.v("CP- sendDelete to " + port, selection);
    }

    private int delLocal() {

        int count = CV.size();
        synchronized (this) {
            CV.clear();
        }
        return count;

    }

    private int delGlobal() {

        int count = CV.size();

        synchronized (this) {
            CV.clear();
        }

        // Create Global Delete message to delete all <key,value> pairs from all
        // nodes
        for (String port : portGroup) {
            if (!port.equals(myPort)) {
                NodeMessage globalDelete = new NodeMessage();
                globalDelete.type = "globaldelete";
                globalDelete.myPort = myPort;
                globalDelete.sendPort = port;

                ClientThread delete = new ClientThread(globalDelete);
                Thread t = new Thread(delete);
                t.start();

                Log.v("CP-delGlobal - Send Global Delete to ", port);
            }
        }

        return count;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public synchronized Uri insert(Uri uri, ContentValues values) {

        String key = values.getAsString(COLUMN_1);
        String value = values.getAsString(COLUMN_2);
        String[] keyvalue = {
                key, value
        };
        String partitionPort = getPartitionPort(key);

        if (partitionPort.equals(myPort)) {

            Log.v("CP- Local Insert", values.toString());

            // Insert on own node and chain replicate on successors
            CV.put(key, value);
            sendMsg("replicate1", keyvalue, successor1);

        } else {

            // Directly send the insert message on the respective node and let
            // it do it's replication, if not alive do its replication in
            // ClientThread
            sendMsg("insert", keyvalue, partitionPort);

        }

        return uri;
    }

    public void sendMsg(String type, String[] keyvalue, String port) {

        NodeMessage insertMsg = new NodeMessage();
        insertMsg.type = type;
        insertMsg.myPort = myPort;
        insertMsg.sendPort = port;
        insertMsg.cv = keyvalue;

        ClientThread insert = new ClientThread(insertMsg);
        Thread t = new Thread(insert);
        t.start();

        Log.v("CP- sendMsg " + type + " to " + port, keyvalue[0] + ", " + keyvalue[1]);

    }

    @Override
    public synchronized Cursor query(Uri uri, String[] projection, String selection,
            String[] selectionArgs, String sortOrder) {

        cursor = new MatrixCursor(columnNames);

        if (selection.equals("*")) {

            return getGlobal();

        } else if (selection.equals("@")) {

            return getLocal();

        } else {

            String partitionPort = getPartitionPort(selection);

            if (partitionPort.equals(myPort)) {

                return localQuery(selection, partitionPort);

            } else {

                return sendQuery(selection, partitionPort);

            }

        }

    }

    private MatrixCursor localQuery(String selection, String partitionPort) {

        MatrixCursor temp = new MatrixCursor(columnNames);

        temp.addRow(new String[] {
                selection, CV.get(selection)
        });

        Log.v("CP-localQuery", selection);

        return temp;
    }

    private Cursor sendQuery(String selection, String port) {
        // try {
        Log.v("CP-sendQuery to " + port, selection);

        value = null;

        NodeMessage queryMsg = new NodeMessage();
        queryMsg.type = "query";
        queryMsg.myPort = myPort;
        queryMsg.sendPort = port;
        queryMsg.selection = selection;

        ClientThread query = new ClientThread(queryMsg);
        Thread t = new Thread(query);
        t.start();

        // Wait for a reply
        waitOver = false;
        while (!waitOver) {

        }

        waitOver = true;

        if (value == null) {
            cursor = new MatrixCursor(columnNames);
            String[] successors = getSuc(port);
            NodeMessage queryMsg2 = new NodeMessage();
            queryMsg2.type = "query";
            queryMsg2.myPort = myPort;
            queryMsg2.sendPort = successors[0];
            queryMsg2.selection = selection;

            ClientThread query2 = new ClientThread(queryMsg2);
            Thread t2 = new Thread(query2);
            t2.start();

            // Wait for a reply
            waitOver = false;
            while (!waitOver) {

            }

            waitOver = true;
            value = null;
            return cursor;

        }

        value = null;
        return cursor;
    }

    private MatrixCursor getLocal() {

        MatrixCursor temp = new MatrixCursor(columnNames);

        for (String key : CV.keySet()) {
            Log.v("CP- getLocal", key);

            temp.addRow(new String[] {
                    key, CV.get(key)
            });

        }

        System.out.println("CP- getlocal - Cursor Count" + temp.getCount());
        return temp;
    }

    private Cursor getGlobal() {

        NodeMessage globalQuery = new NodeMessage();
        globalQuery.type = "globalquery";
        globalQuery.globalCursor = new HashMap<String, String>();
        globalQuery.globalCursor.putAll(CV);
        globalQuery.myPort = myPort;
        globalQuery.sendPort = successor1;
        globalQuery.queryPort = myPort;

        ClientThread global = new ClientThread(globalQuery);
        Thread t = new Thread(global);
        t.start();

        Log.v("CP- getGlobal - Send GlobalQuery to " + successor1, "Map Size "
                + globalQuery.globalCursor.size());

        waitOver = false;
        while (!waitOver) {

        }
        waitOver = true;

        Log.v("CP-getGlobal - GlobalQuery done ", "Map Size "
                + cursor.getCount());

        return cursor;
    }

    @SuppressWarnings("unused")
    @Override
    public boolean onCreate() {
        isLoading = true;

        try {
            portGroup = new String[] {
                    REMOTE_PORT4, REMOTE_PORT1, REMOTE_PORT0, REMOTE_PORT2, REMOTE_PORT3
            };

            context = getContext();
            ident = new Identifier(context);

            myPort = getMyPort();
            myID = genHash(String.valueOf(Integer.parseInt(myPort) / 2));

            portIDGroup = new String[] {
                    genHash(REMOTE_PORT4ID), genHash(REMOTE_PORT1ID),
                    genHash(REMOTE_PORT0ID), genHash(REMOTE_PORT2ID),
                    genHash(REMOTE_PORT3ID)
            };

            formRing();
            successor1ID = genHash(String.valueOf(Integer.parseInt(successor1) / 2));
            successor2ID = genHash(String.valueOf(Integer.parseInt(successor2) / 2));

            predecessor1ID = genHash(String.valueOf(Integer.parseInt(predecessor1) / 2));
            predecessor2ID = genHash(String.valueOf(Integer.parseInt(predecessor2) / 2));

            BufferedReader reader = null;

            try {

                File file = new File(context.getFilesDir().getAbsolutePath());
                reader = new BufferedReader(new FileReader(file + "/" + "RESTART"));
                String value = reader.readLine();

                reader.close();

                Log.v("CP- onCreate- Check for Restart", "file read successful (is restart)");

                isRestart = true;

            } catch (Exception e) {
                Log.e("CP- oncreate- Check for Restart",
                        "file read failed, first run (not restart)");
                try {

                    File file = new File(context.getFilesDir().getAbsolutePath(), "RESTART");
                    FileWriter fileWrite = new FileWriter(file);
                    fileWrite.write("RESTART");
                    fileWrite.close();
                    isRestart = false;

                } catch (Exception e1) {
                    Log.e("CP- onCreate-restart-filewrite", "file write failed");
                    e.printStackTrace();
                }

            }

            if (isRestart) {
                Log.v("CP- onCreate", "Node Recovering..");

                // Node Recovery Logic
                getSelf();
                getReplicas();

                Log.v("CP- onCreate", "Node Recovering Done");

            } else {

                Log.v("CP- onCreate", "Normal Run (No Node Recover)");

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        ServerThread server = new ServerThread();
        Thread serverThread = new Thread(server);
        serverThread.start();

        isLoading = false;

        return true;

    }

    private void getSelf() {

        Log.v("onCreate- getSelf1", successor1);
        NodeMessage recover1 = new NodeMessage();
        recover1.type = "recoverself";
        recover1.myPort = myPort;
        recover1.sendPort = successor1;
        recover1.preID = predecessor1ID;
        ClientThread rec1 = new ClientThread(recover1);
        Thread t1 = new Thread(rec1);
        t1.start();
        // waitOver = false;
        // while (!waitOver) {
        //
        // }
        waitOver = true;
        Log.v("onCreate-getSelf1", "Done for " + successor1);

        Log.v("onCreate- getSelf2", successor2);
        NodeMessage recover2 = new NodeMessage();
        recover2.type = "recoverself";
        recover2.myPort = myPort;
        recover2.sendPort = successor2;
        recover2.preID = predecessor1ID;
        ClientThread rec2 = new ClientThread(recover2);
        Thread t2 = new Thread(rec2);
        t2.start();
        // waitOver = false;
        // while (!waitOver) {
        //
        // }
        // waitOver = true;
        Log.v("onCreate- getSelf2", "Done for " + successor2);
    }

    private void getReplicas() {

        Log.v("onCreate- getReplica1", predecessor1);
        NodeMessage replica1 = new NodeMessage();
        replica1.type = "recoverreplica";
        replica1.myPort = myPort;
        replica1.sendPort = predecessor1;
        replica1.preID = predecessor1ID;
        ClientThread rep1 = new ClientThread(replica1);
        Thread t1 = new Thread(rep1);
        t1.start();
        // waitOver = false;
        // while (!waitOver) {
        //
        // }
        // waitOver = true;
        Log.v("onCreate- getReplica1", "Done for " + predecessor1);

        Log.v("onCreate- getReplica2", predecessor2);
        NodeMessage replica2 = new NodeMessage();
        replica2.type = "recoverreplica";
        replica2.myPort = myPort;
        replica2.sendPort = predecessor2;
        replica2.preID = predecessor2ID;
        ClientThread rep2 = new ClientThread(replica2);
        Thread t2 = new Thread(rep2);
        t2.start();
        // waitOver = false;
        // while (!waitOver) {
        //
        // }
        // waitOver = true;
        Log.v("onCreate- getReplica2", "Done for " + predecessor2);

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
            String[] selectionArgs) {
        return 0;
    }

    @SuppressWarnings({
            "resource"
    })
    private String genHash(String input) {

        MessageDigest sha1 = null;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    /*
     * Calculate the port number that this AVD listens on.
     */
    public String getMyPort() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(
                Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        return myPort;
    }

    private String getPartitionPort(String filename) {

        String key = genHash(filename);
        int count = 0;
        int last = portGroup.length - 1;
        for (String ID : portIDGroup) {
            if (count == 0) {
                if (key.compareTo(ID) <= 0 || key.compareTo(portIDGroup[last]) > 0) {
                    return portGroup[count];
                }
            } else {
                if (key.compareTo(ID) <= 0 && key.compareTo(portIDGroup[count - 1]) > 0) {
                    return portGroup[count];
                }
            }
            count++;
        }
        return null;
    }

    public void formRing() {

        if (myPort.equals("11124")) {
            successor1 = "11112";
            successor2 = "11108";
            predecessor1 = "11120";
            predecessor2 = "11116";
        } else if (myPort.equals("11112")) {
            successor1 = "11108";
            successor2 = "11116";
            predecessor1 = "11124";
            predecessor2 = "11120";
        } else if (myPort.equals("11108")) {
            successor1 = "11116";
            successor2 = "11120";
            predecessor1 = "11112";
            predecessor2 = "11124";
        } else if (myPort.equals("11116")) {
            successor1 = "11120";
            successor2 = "11124";
            predecessor1 = "11108";
            predecessor2 = "11112";
        } else if (myPort.equals("11120")) {
            successor1 = "11124";
            successor2 = "11112";
            predecessor1 = "11116";
            predecessor2 = "11108";
        }

    }

    public static String[] getSuc(String port) {

        String[] successors = new String[2];

        if (port.equals("11124")) {
            successors[0] = "11112";
            successors[1] = "11108";
        } else if (port.equals("11112")) {
            successors[0] = "11108";
            successors[1] = "11116";
        } else if (port.equals("11108")) {
            successors[0] = "11116";
            successors[1] = "11120";
        } else if (port.equals("11116")) {
            successors[0] = "11120";
            successors[1] = "11124";
        } else if (port.equals("11120")) {
            successors[0] = "11124";
            successors[1] = "11112";
        }

        return successors;
    }

}
