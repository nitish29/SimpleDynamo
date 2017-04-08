package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    static String myPort;
    private static final String TAG = SimpleDynamoProvider.class.getName();
    static final int SERVER_PORT = 10000;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    ArrayList<Node> nodeList = new ArrayList<Node>(5);
    String[] avdList = {"5562", "5556", "5554", "5558", "5560"};
    String[] portList = { "11124", "11112", "11108", "11116", "11120"};
    String[] hashList = new String[5];
    HashMap<String,Message> keyVersionMap = new HashMap<String, Message>();
    static boolean receivedData = false;
    ArrayList<String> dataReceivedFromNode = new ArrayList<String>();

	@Override
	/*public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}*/
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        if ( !selection.equals("@") && !selection.equals("*") ) {

            //new File(uri.toString()).delete();
            if (getContext().deleteFile(selection)) {

                return 1;

            } else {

                Log.d("Del-Not Working-File:", selection);

            }


        } else if ( selection.equals("@") ) {

            try {

                int count = 0;

                Log.d(TAG, "inside @ delete");

                String[] filenames = getContext().fileList();

                for ( String name : filenames ) {


                    File dir = getContext().getFilesDir();
                    File file = new File(dir, name);
                    boolean deleted = file.delete();


                    if (deleted) {

                        count = count + 1;

                    } else {

                        Log.d("Delete", "Nahi chala");

                    }


                }
                return count;

            } catch (Exception e) {

                Log.e(TAG, "File delete failed");

            }


        } else {

            try {

                int count = 0;

                Log.d(TAG, "inside * delete");

                String[] filenames = getContext().fileList();

                for ( String name : filenames ) {


                    File dir = getContext().getFilesDir();
                    File file = new File(dir, name);
                    boolean deleted = file.delete();


                    if (deleted) {

                        count = count + 1;

                    } else {

                        Log.d("Delete", "Nahi chala");

                    }


                }
                return count;

            } catch (Exception e) {

                Log.e(TAG, "File delete failed");

            }


        }

        return 0;
    }



	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

        //need to get ownport value also
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.d("port", myPort);

        int i;

        for ( i = 0; i < portList.length; i++ ) {

            try {

                if ( i == 0 ) {


                    Node newNode = new Node();
                    newNode.avdNum = avdList[0];
                    newNode.ownPort = portList[0];
                    newNode.hashOwnAvd = genHash(avdList[0]);
                    newNode.predecessorPort = portList[4];
                    newNode.hashOfPredecessor = genHash(avdList[4]);
                    newNode.successorOnePort = portList[i+1];
                    newNode.hashOfSuccessorOne = genHash(avdList[i+1]);
                    newNode.successorTwoPort = portList[i+2];
                    newNode.hashOfSuccessorTwo = genHash(avdList[i+2]);
                    nodeList.add(0, newNode);

                } else if ( i == 4 ) {

                    Node newNode = new Node();
                    newNode.avdNum = avdList[4];
                    newNode.ownPort = portList[4];
                    newNode.hashOwnAvd = genHash(avdList[4]);
                    newNode.predecessorPort = portList[i-1];
                    newNode.hashOfPredecessor = genHash(avdList[i-1]);
                    newNode.successorOnePort = portList[0];
                    newNode.hashOfSuccessorOne = genHash(avdList[0]);
                    newNode.successorTwoPort = portList[1];
                    newNode.hashOfSuccessorTwo = genHash(avdList[1]);
                    nodeList.add(4, newNode);


                } else if ( i == 3 ) {

                    Node newNode = new Node();
                    newNode.avdNum = avdList[3];
                    newNode.ownPort = portList[3];
                    newNode.hashOwnAvd = genHash(avdList[3]);
                    newNode.predecessorPort = portList[i-1];
                    newNode.hashOfPredecessor = genHash(avdList[i-1]);
                    newNode.successorOnePort = portList[i+1];
                    newNode.hashOfSuccessorOne = genHash(avdList[i+1]);
                    newNode.successorTwoPort = portList[0];
                    newNode.hashOfSuccessorTwo = genHash(avdList[0]);
                    nodeList.add(3, newNode);

                } else {

                    Node newNode = new Node();
                    newNode.avdNum = avdList[i];
                    newNode.ownPort = portList[i];
                    newNode.hashOwnAvd = genHash(avdList[i]);
                    newNode.predecessorPort = portList[i-1];
                    newNode.hashOfPredecessor = genHash(avdList[i-1]);
                    newNode.successorOnePort = portList[i+1];
                    newNode.hashOfSuccessorOne = genHash(avdList[i+1]);
                    newNode.successorTwoPort = portList[i+2];
                    newNode.hashOfSuccessorTwo = genHash(avdList[i+2]);
                    nodeList.add(i, newNode);

                }

            } catch (NoSuchAlgorithmException e) {

                Log.d(TAG, "Error Generating Hash Value");

            }


        }

        int j;

        for (j=0; j < avdList.length; j++) {


            try {

                hashList[j] = genHash(avdList[j]);


            } catch ( NoSuchAlgorithmException e ) {

                Log.e(TAG, "error generating avd hashes");

            }

        }


        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);


        } catch ( IOException e ) {

            Log.e(TAG, "can't create a server socket");

        }

        //new asyn tasks that asks's successor for all the keys , then deletes all it's files and replaces all the values

        String message = "retrieve_mine" + "@" + myPort + '\n';
        String successors = "";
        String predecessor = "";

        //String[] portList = { "11124", "11112", "11108", "11116", "11120"};

        if (myPort.equals("11124")) {

            successors = "11112-11108";
            predecessor =  "11120-11116";

        } else if (myPort.equals("11112")) {

            successors = "11108-11116";
            predecessor = "11124-11120";

        } else if (myPort.equals("11108")) {

            successors = "11116-11120";
            predecessor = "11112-11124";

        } else if (myPort.equals("11116")) {

            successors = "11120-11124";
            predecessor = "11108-11112";

        }  else {

            successors = "11124-11112";
            predecessor = "11116-11108";

        }

        new RetrieveKeyTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message, successors, predecessor);

		return false;
	}

    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            Socket socket;

            try {

                while( true ) {

                    socket = serverSocket.accept();
                    Log.d(TAG, "inside-server-task");

                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String message = "null";
                    String unformattedMessage = "";
                    String[] messageData = new String[0];

                    message = in.readLine();
                    Log.d("Message-Server", message);
                    unformattedMessage = message.trim();
                    messageData = unformattedMessage.split("@");

                    Log.d(TAG, messageData[0]);

                    if (messageData[0].equals("insert") ) {

                        String key = messageData[1];
                        String value = messageData[2];
                        String returnPort = messageData[3];
                        String coordinatorPort = messageData[4];

                        Log.d(TAG, "***InsideInsert****");
                        Log.d("Key", key);
                        Log.d("Value", value);
                        Log.d("ReturnPort", returnPort);
                        Log.d("CoordinatorPort", coordinatorPort);
                        Log.d(TAG, "***InsideInsert****");


                        if ( keyVersionMap.get(key) == null ) {

                            Message details = new Message();
                            details.coordinatorPort = coordinatorPort;
                            details.versionNumber = 1;
                            Log.d("NewInsert-KVMapCDPort", details.coordinatorPort);

                            keyVersionMap.put(key, details);

                            String filename = key;
                            FileOutputStream outputStream;

                            try {
                                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                                outputStream.write(value.getBytes());
                                outputStream.close();
                            } catch (Exception e) {
                                Log.e(TAG, "File write failed");
                            }

                            Log.d("Insert-Server-New", "Key:" + key + ", values:" + value);


                        } else {

                            Message details = keyVersionMap.get(key);
                            details.versionNumber = details.versionNumber + 1;
                            //don't need it
                            Log.d("UpdInsert-KVMapCDPort", details.coordinatorPort);
                            details.coordinatorPort = coordinatorPort;
                            keyVersionMap.put(key, details);

                            if ( getContext().deleteFile(key) ) {


                                String filename = key;
                                FileOutputStream outputStream;

                                try {
                                    outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                                    outputStream.write(value.getBytes());
                                    outputStream.close();
                                } catch (Exception e) {
                                    Log.e(TAG, "File write failed");
                                }

                                Log.d("Insert-Server-Upd", "Key:" + key + ", values:" + value);


                            } else {

                                Log.d("Error-updating-file", key);

                            }


                        }

                        String sendmessage = "key_inserted" + "@" + key + '\n';
                        Log.d("SendMessageFormat", sendmessage);
                        PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                        out.print(sendmessage);
                        out.flush();


                    } else if (messageData[0].equals("replicate") ) {


                        String key = messageData[1];
                        String value = messageData[2];
                        String returnPort = messageData[3];
                        String coordinatorPort = messageData[4];

                        Log.d(TAG, "***InsideReplicate****");
                        Log.d("Key", key);
                        Log.d("Value", value);
                        Log.d("ReturnPort", returnPort);
                        Log.d("CoordinatorPort", coordinatorPort);
                        Log.d(TAG, "***InsideInsert****");

                        if ( keyVersionMap.get(key) == null ) {

                            Message details = new Message();
                            details.coordinatorPort = coordinatorPort;
                            details.versionNumber = 1;
                            Log.d("NewRep-EntryKVMapCDPort", details.coordinatorPort);

                            keyVersionMap.put(key, details);

                            String filename = key;
                            FileOutputStream outputStream;

                            try {
                                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                                outputStream.write(value.getBytes());
                                outputStream.close();
                            } catch (Exception e) {
                                Log.e(TAG, "File write failed");
                            }

                            Log.d("Insert-Server-New", "Key:" + key + ", values:" + value);


                        } else {

                            Message details = keyVersionMap.get(key);
                            Log.d("UpdRep-EntryKVMapCDPort",details.coordinatorPort);
                            details.versionNumber = details.versionNumber + 1;
                            details.coordinatorPort = coordinatorPort;
                            keyVersionMap.put(key, details);

                            if ( getContext().deleteFile(key) ) {


                                String filename = key;
                                FileOutputStream outputStream;

                                try {
                                    outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                                    outputStream.write(value.getBytes());
                                    outputStream.close();
                                } catch (Exception e) {
                                    Log.e(TAG, "File write failed");
                                }

                                Log.d("Insert-Server-Upd", "Key:" + key + ", values:" + value);


                            } else {

                                Log.d("Error-updating-file", key);

                            }


                        }

                        String sendmessage = "key_inserted" + "@" + key + '\n';
                        Log.d("SendMessageFormat", sendmessage);
                        PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                        out.print(sendmessage);
                        out.flush();


                    } else if (messageData[0].equals("retrieve_key")) {

                        Log.d("Here-inside", "retrieve_key");

                        String key = messageData[1];
                        String returnPort = messageData[2];

                        File file = getContext().getFileStreamPath(key);
                        StringBuffer fileContent = new StringBuffer("");

                        if ( file.exists() ) {

                            Message details = keyVersionMap.get(key);
                            int version_value = details.versionNumber;

                            try {

                                FileInputStream fis;
                                fis = getContext().openFileInput(key);

                                byte[] buffer = new byte[1024];

                                int n;

                                while ((n = fis.read(buffer)) != -1) {

                                    fileContent.append(new String(buffer, 0, n));

                                }
                                Log.d("File Content:", fileContent.toString());


                            } catch (Exception e) {

                                Log.e(TAG, "File read failed");

                            }


                            String sendmessage = "key_found" + "@" + key + "@" + fileContent.toString() + "@" + version_value + '\n';
                            Log.d("SendMessageFormat", sendmessage);

                            PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                            out.print(sendmessage);
                            out.flush();

                        } else {

                            Log.d("File-retrieve", "error");

                        }

                    } else if (messageData[0].equals("retrieve_all")) {


                        Log.d("Inside-ret-all", message);
                        String keyValuePairs = "";
                        FileInputStream fis;



                        String[] filenames = getContext().fileList();

                        if (filenames.length != 0) {


                            StringBuffer fileContent = new StringBuffer("");

                            for ( String name : filenames ) {

                                String[] record = new String[2];


                                Log.d("FileName:", name);
                                fis = getContext().openFileInput(name);

                                byte[] buffer = new byte[1024];

                                int n;

                                while ((n = fis.read(buffer)) != -1) {

                                    fileContent.append(new String(buffer, 0, n));

                                }

                                //record[0] = name;
                                //record[1] = fileContent.toString();
                                //comma separated version number
                                keyValuePairs += name + "," + fileContent.toString() + "-";


                                //reset string buffer
                                fileContent.delete(0, fileContent.length());


                            }
                            String updatedKeyValuePair = keyValuePairs.substring(0, keyValuePairs.length()-1);


                            String sendMessage = updatedKeyValuePair + "@" + myPort + '\n';

                            PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                            out.print(sendMessage);
                            out.flush();

                        } else {

                            String sendMessage = "no_files" + "@" + myPort + '\n';

                            PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                            out.print(sendMessage);
                            out.flush();

                        }



                    } else if ( messageData[0].equals("retrieve_mine") ) {

                            String coordinatorPort = messageData[1];
                            Log.d("coordinatorPort", coordinatorPort);
                            String keyValuePairs = "";
                            String updatedKeyValuePair = "";
                            FileInputStream fis;

                            String[] filenames = getContext().fileList();

                            Log.d("FilenamesLength", String.valueOf(filenames.length));
                            if (filenames.length != 0) {

                                StringBuffer fileContent = new StringBuffer("");

                                for ( String name : filenames ) {

                                    Log.d("FileNameCheck:", name);

                                    String[] record = new String[2];

                                   //Message details = keyVersionMap.get(name);
                                    //TODO: BREAKS OVER HERE SOMETIMES
                                    //Log.d("messagestoredportpn", String.valueOf(details.toString()));
                                    //Log.d("messagestoredportpn", String.valueOf(details.coordinatorPort));

                                    Node coordinatorNodeForCurrentFile = getPreferenceNodeDetails(name);

                                    if (coordinatorNodeForCurrentFile.ownPort.equals(coordinatorPort)) {

                                        Message details = keyVersionMap.get(name);
                                        //int version_number = details.versionNumber;

                                        Log.d("FileName:", name);
                                        fis = getContext().openFileInput(name);

                                        byte[] buffer = new byte[1024];

                                        int n;

                                        while ((n = fis.read(buffer)) != -1) {

                                            fileContent.append(new String(buffer, 0, n));

                                        }

                                        keyValuePairs += name + "," + fileContent.toString() + "," + String.valueOf(1) + "-";

                                        //reset string buffer
                                        fileContent.delete(0, fileContent.length());


                                    } else {

                                        continue;

                                    }

                                    //check if coordinator is the passedCoordinatorPort (basically for every key - do getPreferenceNode details)
                                    //if yes, add me to the list
                                    //else continue

                                    /*if ( details.coordinatorPort.equals(coordinatorPort)) {

                                        Log.d("messagestoredport", details.coordinatorPort);
                                        Log.d("coordinator-port", coordinatorPort);

                                        int version_number = details.versionNumber;

                                        Log.d("FileName:", name);
                                        fis = getContext().openFileInput(name);

                                        byte[] buffer = new byte[1024];

                                        int n;

                                        while ((n = fis.read(buffer)) != -1) {

                                            fileContent.append(new String(buffer, 0, n));

                                        }

                                        //record[0] = name;
                                        //record[1] = fileContent.toString();
                                        //comma separated version number
                                        keyValuePairs += name + "," + fileContent.toString() + "," + String.valueOf(version_number) + "-";

                                        //reset string buffer
                                        fileContent.delete(0, fileContent.length());


                                    } else {

                                        continue;

                                    }*/
                                }
                                Log.d("Key-val-pairs", keyValuePairs);
                                //check laga dena ki key value pair null na ho
                                if ( (keyValuePairs != null)&&(!keyValuePairs.equals("")) ) {
                                    updatedKeyValuePair = keyValuePairs.substring(0, keyValuePairs.length() - 1);
                                }
                                String sendMessage = null;

                                if ( (updatedKeyValuePair != null)&&(!updatedKeyValuePair.equals("")) ) {

                                    sendMessage = updatedKeyValuePair + "@" + myPort + '\n';

                                } else {

                                    sendMessage = "no_files" + "@" + myPort + '\n';

                                }

                                PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                                out.print(sendMessage);
                                out.flush();




                            } else {

                                String sendMessage = "no_files" + "@" + myPort + '\n';

                                PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                                out.print(sendMessage);
                                out.flush();


                            }


                    } else if ( messageData[0].equals("retrieve_your_keys") ) {

                        String returnPort = messageData[1];
                        Log.d("predReturnPort", returnPort);
                        String keyValuePairs = "";
                        String updatedKeyValuePair = "";

                        FileInputStream fis;
                        String[] filenames = getContext().fileList();

                        if (filenames.length != 0) {

                            StringBuffer fileContent = new StringBuffer("");

                            for ( String name : filenames ) {

                                String[] record = new String[2];

                                Message details = keyVersionMap.get(name);
                                //TODO: BREAKS OVER HERE AS WELL
                                Log.d("before-if-con-port", details.coordinatorPort);

                                //check if coordinator is myPort or if coordinator is my predecessor
                                //if yes, add me to the list
                                //else continue


                                if ( details.coordinatorPort.equals(myPort)) {

                                    Log.d("msg-cord-port", details.coordinatorPort);
                                    Log.d("my-port-pred", myPort);

                                    int version_number = details.versionNumber;

                                    Log.d("FileName:", name);
                                    fis = getContext().openFileInput(name);

                                    byte[] buffer = new byte[1024];

                                    int n;

                                    while ((n = fis.read(buffer)) != -1) {

                                        fileContent.append(new String(buffer, 0, n));

                                    }

                                    keyValuePairs += name + "," + fileContent.toString() + "," + String.valueOf(version_number) + "-";

                                    //reset string buffer
                                    fileContent.delete(0, fileContent.length());


                                } else {

                                    continue;

                                }
                            }
                            Log.d("panga=keyval", keyValuePairs);
                            //check laga dena ki key value pair null na ho
                            if ( !keyValuePairs.equals("") ) {
                                updatedKeyValuePair = keyValuePairs.substring(0, keyValuePairs.length() - 1);
                            }
                            String sendMessage = null;

                            if ( updatedKeyValuePair != null ) {

                                sendMessage = updatedKeyValuePair + "@" + myPort + '\n';

                            } else {

                                sendMessage = "no_files" + "@" + myPort + '\n';

                            }

                            PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                            out.print(sendMessage);
                            out.flush();


                        } else {

                            String sendMessage = "no_files" + "@" + myPort + '\n';

                            PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                            out.print(sendMessage);
                            out.flush();


                        }


                    }




                }


            } catch(IOException e) {

                Log.e(TAG, "error receiving messages-Server-Task");

            }

            return null;
        }
    }


    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        Log.d(TAG, "I was called");

        Set<Map.Entry<String, Object>> data = values.valueSet();
        Iterator itr = data.iterator();
        String key = "";
        String value = "";

        while (itr.hasNext()) {

            Map.Entry me = (Map.Entry)itr.next();
            value = me.getValue().toString();
            me = (Map.Entry)itr.next();
            key = me.getValue().toString();

            Log.d("key-", key);
            Log.d("value-", value);

        }

        Node coordinatorNodeForKey = getPreferenceNodeDetails(key);
        Log.d(TAG, "***BeforeCheckingInsertConditions****");
        Log.d("CoordinatorPort", coordinatorNodeForKey.ownPort);
        Log.d("Node", coordinatorNodeForKey.avdNum);
        Log.d(TAG, "***BeforeCheckingInsertConditions****");

        if ( coordinatorNodeForKey.ownPort.equals(myPort) ) {

            if ( keyVersionMap.get(key) == null ) {

                Message details = new Message();
                details.coordinatorPort = myPort;
                details.versionNumber = 1;

                keyVersionMap.put(key, details);
                Log.d("InsideInsertCdPMyP", details.coordinatorPort);

                String filename = key;
                FileOutputStream outputStream;

                try {
                    outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                    outputStream.write(value.getBytes());
                    outputStream.close();
                } catch (Exception e) {
                    Log.e(TAG, "File write failed");
                }

                Log.d("Testing Insert", "Key:" + key + ", values:" + value);
                Log.v("insert", values.toString());

            } else {

                Message details = keyVersionMap.get(key);
                details.versionNumber = details.versionNumber + 1;
                Log.d("InsideUpdInsertCdPMyP", details.coordinatorPort);
                details.coordinatorPort = coordinatorNodeForKey.ownPort;
                keyVersionMap.put(key, details);

                if ( getContext().deleteFile(key) ) {


                    String filename = key;
                    FileOutputStream outputStream;

                    try {
                        outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                        outputStream.write(value.getBytes());
                        outputStream.close();
                    } catch (Exception e) {
                        Log.e(TAG, "File write failed");
                    }

                    Log.d("Testing Insert", "Key:" + key + ", values:" + value);
                    Log.v("insert-updated", values.toString());


                } else {

                    Log.d("Error-updating-file", key);

                }


            }

            Log.d("Before-FwdRepReqToSucc", "******");
            Log.d("CoordinatorPort", coordinatorNodeForKey.ownPort);
            Log.d("Succ1Port", coordinatorNodeForKey.successorOnePort);
            Log.d("Succ2Port", coordinatorNodeForKey.successorTwoPort);
            Log.d("Before-FwdRepReqToSucc", "******");


            //forward request to my two successor nodes as well
            String message2 = "replicate" + "@" + key + "@" + value + "@" + myPort + "@" + coordinatorNodeForKey.ownPort +'\n';
            String[] successorPorts = { coordinatorNodeForKey.successorOnePort, coordinatorNodeForKey.successorTwoPort};
            for ( int j = 0; j < successorPorts.length; j++ ) {

                try {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successorPorts[j]));

                    socket.setSoTimeout(4000);

                    PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                    out.print(message2);
                    out.flush();

                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String responseMessage = in.readLine();

                    if ( responseMessage == null ) {


                        Log.d("OwnSuccNullReplicateOn:", successorPorts[j]);
                        continue;

                    } else {

                        Log.d("ResponseOwnSuccInsert:", responseMessage);

                    }


                    socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "Own successor insert failed");
                    continue;

                } catch (IOException e) {

                    Log.e(TAG, "IOException-For-own-query-succ" + successorPorts[j]);
                    continue;

                }

            }

            //new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message2, coordinatorNodeForKey.successorOnePort);
            //new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message2, coordinatorNodeForKey.successorTwoPort);


        } else {

            String message = "insert" + "@" + key + "@" + value + "@" + myPort + "@" +coordinatorNodeForKey.ownPort +'\n';

            try {

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(coordinatorNodeForKey.ownPort));

                socket.setSoTimeout(4000);

                PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                out.print(message);
                out.flush();

                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String responseMessage = in.readLine();

                if ( responseMessage == null ) {

                    Log.d("ActSuccNullReplicateOn", coordinatorNodeForKey.ownPort);

                }


                socket.close();

            } catch (UnknownHostException e) {
                Log.e("InsertActUnknwnHExcep", coordinatorNodeForKey.ownPort);


            } catch (IOException e) {

                Log.e(TAG, "Insert-exc-coord-act-node" + coordinatorNodeForKey.ownPort);

            }



            String message2 = "replicate" + "@" + key + "@" + value + "@" + myPort + "@" +coordinatorNodeForKey.ownPort + '\n';
            //new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message, coordinatorNodeForKey.ownPort);
            //new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message2, coordinatorNodeForKey.successorOnePort);
            //new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message2, coordinatorNodeForKey.successorTwoPort);
            //forward this message to coordinator and to it's two successors as well

            String[] successorPorts = { coordinatorNodeForKey.successorOnePort, coordinatorNodeForKey.successorTwoPort};
            for ( int j = 0; j < successorPorts.length; j++ ) {

                try {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successorPorts[j]));

                    socket.setSoTimeout(4000);

                    PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                    out.print(message2);
                    out.flush();

                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String responseMessage = in.readLine();

                    if ( responseMessage == null ) {


                        Log.d("NullReplicateOnActSucc:", successorPorts[j]);
                        continue;

                    }


                    socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "null-replicate-on-act-succ:" + successorPorts[j]);
                    continue;

                } catch (IOException e) {

                    Log.e(TAG, "IOException-For-act-query-succ" + successorPorts[j]);
                    continue;

                }

            }




        }



        return null;
    }

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		// TODO Auto-generated method stub

        String[] columns = new String[2];
        columns[0] = "key";
        columns[1] = "value";
        StringBuffer fileContent = new StringBuffer("");

        MatrixCursor cursor = new MatrixCursor(columns);
        MatrixCursor.RowBuilder builder;

        if( !selection.equals("@") && !selection.equals("*") ) {

            Node coordinatorNode = getPreferenceNodeDetails(selection);
            Log.d(TAG,"****InsideQuery***");
            Log.d("CoordinatorPort", coordinatorNode.ownPort);
            Log.d("Successor1Port", coordinatorNode.successorOnePort);
            Log.d("Successor2Port", coordinatorNode.successorTwoPort);
            Log.d(TAG,"****InsideQuery***");

            if ( coordinatorNode.ownPort.equals(myPort) ) {

                Log.d("CDPortEqualsMyPort", myPort);
                Log.d("CDPortEqualsCDPort", coordinatorNode.ownPort);


                File file = getContext().getFileStreamPath(selection);

                if ( file.exists() ) {

                    Message details = keyVersionMap.get(selection);

                    Log.d("QueryCDPortEqualsMyPort", myPort);
                    Log.d("CDPortFromDetailMsg", details.coordinatorPort);



                    int coordinatorVersionVal = details.versionNumber;
                    int versionValueChecker = coordinatorVersionVal;
                    String fileValue = "";

                    try {


                        FileInputStream fis;
                        fis = getContext().openFileInput(selection);

                        byte[] buffer = new byte[1024];

                        int n;

                        while ((n = fis.read(buffer)) != -1) {

                            fileContent.append(new String(buffer, 0, n));

                        }
                        Log.d("File Content:", fileContent.toString());

                        //builder = cursor.newRow();
                        //builder.add(selection);
                        //builder.add(fileContent);

                        //Log.v("query", selection);
                        //cursor.close();
                        //return cursor;

                        fileValue = fileContent.toString();
                        String message = "retrieve_key" + "@" + selection + "@" + myPort + '\n';


                        String[] successorPorts = { coordinatorNode.successorOnePort, coordinatorNode.successorTwoPort};

                        for ( int j = 0; j < successorPorts.length; j++ ) {

                            try {

                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(successorPorts[j]));

                                socket.setSoTimeout(4000);

                                PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                                out.print(message);
                                out.flush();

                                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                String responseMessage = in.readLine();

                                if ( responseMessage == null ) {


                                    Log.d("response-retrieve-port:", successorPorts[j]);
                                    continue;

                                } else {

                                    String unformattedMessage = responseMessage.trim();
                                    String[] messageData = unformattedMessage.split("@");

                                    int successorVersionVal = Integer.valueOf(messageData[3]);

                                    if ( successorVersionVal >= versionValueChecker ) {

                                        fileValue = messageData[2];
                                        versionValueChecker = successorVersionVal;

                                    }


                                }


                                socket.close();

                            } catch (UnknownHostException e) {
                                Log.e(TAG, "SendJoinRequestTask UnknownHostException");

                            } catch (IOException e) {

                                Log.e(TAG, "IOException-For-query-succ" + successorPorts[j]);

                            }


                        }

                        builder = cursor.newRow();
                        builder.add(selection);
                        builder.add(fileValue);
                        Log.v("query", selection);
                        cursor.close();
                        return cursor;



                    } catch (Exception e) {

                        Log.e(TAG, "File read failed");

                    }



                } else {

                    Log.d("Error-should-have-file", "but-don't-have-it");

                }


            } else {

                Log.d("Asli-node", "se pucho");

                String[] allPorts = { coordinatorNode.ownPort, coordinatorNode.successorOnePort, coordinatorNode.successorTwoPort};
                String message = "retrieve_key" + "@" + selection + "@" + myPort + '\n';

                int versionValueChecker = 0;
                String fileValue = "";

                for ( int j = 0; j < allPorts.length; j++ ) {

                    try {

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(allPorts[j]));

                        socket.setSoTimeout(4000);



                        PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                        out.print(message);
                        out.flush();

                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String responseMessage = in.readLine();

                        if ( responseMessage == null ) {

                            Log.d("response-retrieve-key:", "null" + allPorts[j]);
                            continue;

                        } else {

                            Log.d("response-retrieve-key:", responseMessage);

                            String unformattedMessage = responseMessage.trim();
                            String[] messageData = unformattedMessage.split("@");

                            int successorVersionVal = Integer.valueOf(messageData[3]);

                            if ( successorVersionVal >= versionValueChecker ) {

                                fileValue = messageData[2];
                                versionValueChecker = successorVersionVal;

                            }


                        }



                        socket.close();

                    } catch (UnknownHostException e) {
                        Log.e(TAG, "SendJoinRequestTask UnknownHostException");

                    } catch (IOException e) {

                        Log.e(TAG, "IOExceptionFor-query-rel-node" + allPorts[j]);

                    }


                }

                builder = cursor.newRow();
                builder.add(selection);
                builder.add(fileValue);
                Log.v("query", selection);
                cursor.close();
                return cursor;

            }


        } else if ( selection.equals("@")) {


            try {


                Log.d("Selection:", selection);

                FileInputStream fis;

                String[] filenames = getContext().fileList();

                for ( String name : filenames ) {

                    String[] record = new String[2];


                    Log.d("FileName:", name);
                    fis = getContext().openFileInput(name);

                    byte[] buffer = new byte[1024];

                    int n;

                    while ((n = fis.read(buffer)) != -1) {

                        fileContent.append(new String(buffer, 0, n));

                    }

                    record[0] = name;
                    record[1] = fileContent.toString();

                    //reset string buffer
                    fileContent.delete(0, fileContent.length());


                    cursor.addRow(record);


                }

            } catch (Exception e) {

                Log.e(TAG, "File read failed");

            }



        } else {

            Log.d("Inside-Star", "case");

            try {

                FileInputStream fis;

                String[] filenames = getContext().fileList();

                for ( String name : filenames ) {

                    String[] record = new String[2];


                    Log.d("FileName:", name);
                    fis = getContext().openFileInput(name);

                    byte[] buffer = new byte[1024];

                    int n;

                    while ((n = fis.read(buffer)) != -1) {

                        fileContent.append(new String(buffer, 0, n));

                    }

                    record[0] = name;
                    record[1] = fileContent.toString();

                    //reset string buffer
                    fileContent.delete(0, fileContent.length());


                    cursor.addRow(record);


                }

            } catch (Exception e) {

                Log.e(TAG, "File read failed");

            }

            for ( int j=0; j < portList.length; j++ ) {

                if (!portList[j].equals(myPort)) {

                    try {

                        String message = "retrieve_all" + "@" + myPort + '\n';
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portList[j]));

                        PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                        out.print(message);
                        out.flush();

                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String responseMessage = in.readLine();

                        if ( responseMessage == null ) {

                            Log.d("Starcase-query-fail", portList[j]);
                            continue;

                        } else {

                            Log.d("Message response:", responseMessage);

                            String unformattedMessage = responseMessage.trim();
                            String[] messageData = unformattedMessage.split("@");

                            if ( !messageData[0].equals("no_files") ) {


                                String keyValuePairsString = messageData[0];

                                Log.d("Key-Value-String", keyValuePairsString);

                                String[] keyValuePairArray = keyValuePairsString.split("-");

                                for (String pair : keyValuePairArray ) {

                                    Log.d("Pair", pair);

                                    String[] pairSplit = pair.split(",");

                                    builder = cursor.newRow();
                                    builder.add(pairSplit[0]);
                                    builder.add(pairSplit[1]);

                                }


                            }



                        }




                    } catch (UnknownHostException e) {
                        Log.e(TAG, "UnknownHostException");
                    } catch (IOException e) {
                        Log.e(TAG, "IO Exception For-retall-@" + portList[j]);
                    }


                }


            }


        }


		return cursor;
	}

    public Node getPreferenceNodeDetails( String key ) {


        Node currentNode = null;

        try {

            String hashOfKey = genHash(key);

            int i;

            for ( i=0; i < nodeList.size(); i++ ) {
                currentNode = null;
                currentNode = nodeList.get(i);
                //Log.d("Node", currentNode.avdNum);

                if ( hashOfKey.compareTo(currentNode.hashOfPredecessor) > 0 && i == 0 ) {

                    //Log.d("Key", key);
                    //Log.d("Hash-key", hashOfKey);
                    //Log.d("hash-predecessor", currentNode.hashOfPredecessor);
                    return currentNode;

                } else if ( hashOfKey.compareTo(currentNode.hashOwnAvd) <= 0) {

                    //Log.d("Here", "in else condition");
                    //Log.d("Key", key);
                    //Log.d("Hash-key", hashOfKey);
                    //Log.d("hash-predecessor", currentNode.hashOfPredecessor);
                    return currentNode;

                }


            }


        } catch (NoSuchAlgorithmException e ) {

            Log.d(TAG, "error generating hash of key");

        }

        return currentNode;
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            Log.d("Message", msgs[0]);
            Log.d("port", msgs[1]);


            try {

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[1]));

                socket.setSoTimeout(4000);

                PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                out.print(msgs[0]);
                out.flush();

                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String responseMessage = in.readLine();

                if ( responseMessage == null ) {

                    Log.d("Message response:", "null aaya");

                } else {

                    Log.d("Message response:", responseMessage);

                }


                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "SendJoinRequestTask UnknownHostException");
            } catch (SocketTimeoutException e) {

                Log.e(TAG, "timeout-for-" + msgs[1]);

            } catch (IOException e) {
                Log.e(TAG, "IO Exception For" + msgs[1]);
            }


            return null;
        }
    }

    private class RetrieveKeyTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

/*
            String[] filenames = getContext().fileList();
            int count = 0;

            for ( String name : filenames ) {


                File dir = getContext().getFilesDir();
                File file = new File(dir, name);
                boolean deleted = file.delete();


                if (deleted) {

                    count = count + 1;

                } else {

                    Log.d("Delete", "Nahi chala");

                }


            }
            //clear the hashmap
            keyVersionMap.clear();*/



            Log.d("Message", msgs[0]);
            Log.d("port", msgs[1]);
            Log.d("predecessor", msgs[2]);

            String unformattedMessage = "";
            String[] ports = new String[0];

            unformattedMessage = msgs[1].trim();
            ports = unformattedMessage.split("-");
            //add the predecessor as well to the calling ports

            String unformattedPorts = msgs[2].trim();
            String [] predecessorPorts = new String[0];
            predecessorPorts = unformattedPorts.split("-");




                for ( int j=0; j < predecessorPorts.length; j++ ) {

                    try {


                        String predecessorMessage = "retrieve_your_keys" + "@" + myPort;

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(predecessorPorts[j]));

                        socket.setSoTimeout(4000);

                        PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                        out.print(predecessorMessage);
                        out.flush();

                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String responseMessage = in.readLine();

                        if ( responseMessage == null ) {

                            Log.d("Retrieve-Task-Pred", "failed");

                        } else {

                            String unformattedResponseMessage = responseMessage.trim();
                            String[] messageData = unformattedResponseMessage.split("@");

                            if ( !messageData[0].equals("no_files") ) {

                                String keyValuePairsString = messageData[0];

                                Log.d("Key-Value-String", keyValuePairsString);

                                String[] keyValuePairArray = keyValuePairsString.split("-");

                                for (String pair : keyValuePairArray ) {

                                    Log.d("Pair", pair);

                                    String[] pairSplit = pair.split(",");

                                    /*Message details = new Message();
                                    details.coordinatorPort = messageData[1];
                                    details.versionNumber = 1;
                                    keyVersionMap.put(pairSplit[0], details);


                                    String filename = pairSplit[0];
                                    FileOutputStream outputStream;

                                    try {
                                        outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                                        outputStream.write(pairSplit[1].getBytes());
                                        outputStream.close();
                                    } catch (Exception e) {
                                        Log.e(TAG, "File write failed");
                                    }*/

                                    if ( keyVersionMap.get(pairSplit[0]) == null ) {

                                        Message details = new Message();
                                        details.coordinatorPort = messageData[1];
                                        details.versionNumber = 1;
                                        keyVersionMap.put(pairSplit[0], details);


                                        String filename = pairSplit[0];
                                        FileOutputStream outputStream;

                                        try {
                                            outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                                            outputStream.write(pairSplit[1].getBytes());
                                            outputStream.close();
                                        } catch (Exception e) {
                                            Log.e(TAG, "File write failed");
                                        }

                                        Log.d("Insert-Server-New", "Key:" + pairSplit[0] + ", values:" + pairSplit[1]);


                                    } else {


                                        Message details = keyVersionMap.get(pairSplit[0]);
                                        int old_version_number = details.versionNumber;

                                        int response_version_number = Integer.valueOf(pairSplit[2]);

                                        if ( old_version_number <= response_version_number ) {

                                            details.versionNumber = details.versionNumber + 1;
                                            keyVersionMap.put(pairSplit[0], details);

                                            if ( getContext().deleteFile(pairSplit[0])) {


                                                String filename = pairSplit[0];
                                                FileOutputStream outputStream;

                                                try {
                                                    outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                                                    outputStream.write(pairSplit[1].getBytes());
                                                    outputStream.close();
                                                } catch (Exception e) {
                                                    Log.e(TAG, "File write failed");
                                                }

                                                Log.d("Insert-Server-Upd", "Key:" + pairSplit[0] + ", values:" + pairSplit[1]);


                                            } else {

                                                Log.d("Error-updating-file", pairSplit[0]);

                                            }

                                        }


                                    }

                                    Log.d("Insert-Server-New", "Key:" + pairSplit[0] + ", values:" + pairSplit[1]);


                                }

                            }


                        }
                        socket.close();

                    } catch (UnknownHostException e) {
                        Log.e(TAG, "SendJoinRequestTask UnknownHostException");
                        continue;
                    } catch (IOException e) {
                        Log.e(TAG, "IO Exception For" + predecessorPorts[j]);
                        continue;
                    }





                }



            try {

                for (int i = 0; i < ports.length; i++ ) {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(ports[i]));

                    socket.setSoTimeout(4000);

                    PrintWriter out= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                    out.print(msgs[0]);
                    out.flush();

                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String responseMessage = in.readLine();

                    if ( responseMessage == null ) {

                        Log.d("Retrieve-Task", "failed");

                    } else {


                        String unformattedResponseMessage = responseMessage.trim();
                        String[] messageData = unformattedResponseMessage.split("@");

                        if ( !messageData[0].equals("no_files") ) {


                            String keyValuePairsString = messageData[0];

                            Log.d("Key-Value-String", keyValuePairsString);

                            String[] keyValuePairArray = keyValuePairsString.split("-");

                            for (String pair : keyValuePairArray ) {

                                Log.d("Pair", pair);

                                String[] pairSplit = pair.split(",");


                                if ( keyVersionMap.get(pairSplit[0]) == null ) {

                                    Message details = new Message();
                                    details.coordinatorPort = myPort;
                                    details.versionNumber = 1;
                                    keyVersionMap.put(pairSplit[0], details);


                                    String filename = pairSplit[0];
                                    FileOutputStream outputStream;

                                    try {
                                        outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                                        outputStream.write(pairSplit[1].getBytes());
                                        outputStream.close();
                                    } catch (Exception e) {
                                        Log.e(TAG, "File write failed");
                                    }

                                    Log.d("Insert-Server-New", "Key:" + pairSplit[0] + ", values:" + pairSplit[1]);


                                } else {


                                    Message details = keyVersionMap.get(pairSplit[0]);
                                    int old_version_number = details.versionNumber;

                                    int response_version_number = Integer.valueOf(pairSplit[2]);

                                    if ( old_version_number <= response_version_number ) {

                                        details.versionNumber = details.versionNumber + 1;
                                        keyVersionMap.put(pairSplit[0], details);

                                        if ( getContext().deleteFile(pairSplit[0])) {


                                            String filename = pairSplit[0];
                                            FileOutputStream outputStream;

                                            try {
                                                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                                                outputStream.write(pairSplit[1].getBytes());
                                                outputStream.close();
                                            } catch (Exception e) {
                                                Log.e(TAG, "File write failed");
                                            }

                                            Log.d("Insert-Server-Upd", "Key:" + pairSplit[0] + ", values:" + pairSplit[1]);


                                        } else {

                                            Log.d("Error-updating-file", pairSplit[0]);

                                        }

                                    }


                                }


                            }


                        }


                    }

                    socket.close();


                }


            } catch (UnknownHostException e) {
                Log.e(TAG, "SendJoinRequestTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "IO Exception For" + msgs[1]);
            }


            return null;
        }
    }


	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub


		return 0;
	}

    private String getAvdNumberFromPort ( String port ) {

        String avdNumber = "";

        int portNumber = Integer.valueOf(port);
        int avd = portNumber / 2;
        avdNumber = String.valueOf(avd);

        return avdNumber;
    }


    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
