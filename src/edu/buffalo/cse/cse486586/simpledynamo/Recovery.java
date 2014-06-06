
package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

public class Recovery implements Runnable {
    NodeMessage msg;

    public Recovery(NodeMessage msg) {
        this.msg = msg;
    }

    @Override
    public void run() {
        insertAll();
    }

    private void insertAll() {
        long st = System.currentTimeMillis();
        // Log.v("Recovery", "insertAll from " + msg.myPort + ", type- " +
        // msg.largestID + ", " +
        // msg.globalCursor.toString());

        String ID = genHash(String.valueOf(Integer.parseInt(msg.myPort) / 2));

        if (msg.largestID.equals("recoverself")) {
            for (String key : msg.globalCursor.keySet()) {

                if (self(key)) {
                    Log.v("Recovery", "getRecover - Self " + key);
                    synchronized (this) {
                        SimpleDynamoProvider.CV.put(key, msg.globalCursor.get(key));
                    }
                }
            }

        } else if (msg.largestID.equals("recoverreplica")) {

            for (String key : msg.globalCursor.keySet()) {
                if (replica(key, msg.myPort, ID, msg.preID)) {
                    Log.v("Recovery", "getRecover - Replica " + key);
                    synchronized (this) {
                        SimpleDynamoProvider.CV.put(key, msg.globalCursor.get(key));
                    }
                }
            }
        }

        long et = System.currentTimeMillis();
        System.out.println("Time for Recovery - " + (et - st));
    }

    private boolean replica(String selection, String port, String ID, String preID) {
        // Check with MyPort
        String key = genHash(selection);
        int last = SimpleDynamoProvider.portGroup.length - 1;
        if (port.equals(SimpleDynamoProvider.portGroup[0])) {
            if (key.compareTo(ID) <= 0
                    || key.compareTo(SimpleDynamoProvider.portIDGroup[last]) > 0) {
                return true;
            }
        } else {
            if (key.compareTo(ID) <= 0
                    && key.compareTo(preID) > 0) {
                return true;
            }
        }

        return false;
    }

    private boolean self(String selection) {
        // Check with SelfPort i.e. Message Port
        String key = genHash(selection);
        int last = SimpleDynamoProvider.portGroup.length - 1;
        if (SimpleDynamoProvider.myPort.equals(SimpleDynamoProvider.portGroup[0])) {
            if (key.compareTo(SimpleDynamoProvider.myID) <= 0
                    || key.compareTo(SimpleDynamoProvider.portIDGroup[last]) > 0) {
                return true;
            }
        } else {
            if (key.compareTo(SimpleDynamoProvider.myID) <= 0
                    && key.compareTo(SimpleDynamoProvider.predecessor1ID) > 0) {
                return true;
            }
        }

        return false;
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

}
