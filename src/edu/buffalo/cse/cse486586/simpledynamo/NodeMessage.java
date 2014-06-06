
package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Class used to create objects to send accross the network, which contain all
 * the information about that particular message.
 * 
 * @author Vivek Bhalla
 */
public class NodeMessage implements Serializable {

    private static final long serialVersionUID = 1L;
    public String type; // Message Type
    public String myPort; // Port of the sender
    public String preID; // Predecessor of the sender
    public String largestID; // Used to store largest node ID in the ring
    public String sendPort; // Port of the receiver
    public String queryPort; // Port of message that started the query or global query
    public String uri; // Uri used for insert, query, delete (converted to string)
    public String selection; // The key to query or delete
    public String[] cv; // The <key,value> pair to be inserted (cv[0] - key, cv[1]- value)
    public HashMap<String, String> globalCursor = new HashMap<String, String>(); // DataStructure used to store the cursor/matrix cursor

    
}
