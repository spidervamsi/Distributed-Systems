package edu.buffalo.cse.cse486586.simpledht;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;


// command to test
// ./simpledht-grading.linux /home/vamsi/AndroidStudioProjects/SimpleDht/app/build/outputs/apk/debug/app-debug.apk

public class SimpleDhtProvider extends ContentProvider {
    Context context;
    int myPort,leftPort=0,rightPort=0;
    int count = 0;
    int firstPort = 11124;
    ArrayList<Integer> REMOTE_PORTS = new ArrayList<Integer>(Arrays.asList(11108,11112,11116,11120,11124));
    ArrayList<Integer> portsSortedList = new ArrayList<Integer>();
    ArrayList<Integer> portsList = new ArrayList<Integer>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public Uri getUri(){
        Uri.Builder uriB = new Uri.Builder();
        uriB.authority("edu.buffalo.cse.cse486586.groupmessenger2.provider");
        uriB.scheme("content");
        Uri uri = uriB.build();
        return uri;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        try {

            if(!continueOrNot(values.get("key").toString())){
                Log.i("return",values.get("key").toString());
                int targetPort = findMatch(values.get("key").toString());
                Log.i("cursor",Integer.toString(targetPort));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"insert", values.get("key").toString(),values.get("value").toString(),Integer.toString(targetPort));
                return  uri;
            }


            Log.i("inserted", values.get("key").toString());
            String filename = values.get("key").toString()+".txt";
            String msg = values.get("value").toString();

            File file = new File(context.getFilesDir(), filename);

            FileOutputStream fos = context.openFileOutput(filename, Context.MODE_PRIVATE);
            fos.write(msg.getBytes());
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
            Log.i("file","can not write to file");
        }

        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
//try {
////    Log.i("basic check", String.valueOf(genHash("5554").compareTo(genHash("5556")))); // 1
////    Log.i("basic check", String.valueOf(genHash("5556").compareTo(genHash("5558")))); // -47
////    Log.i("basic check", String.valueOf(genHash("5558").compareTo(genHash("5560")))); // -2
////    Log.i("basic check", String.valueOf(genHash("5560").compareTo(genHash("5562")))); // 50
//
////        HashMap<String,Integer> portsHashValue = new HashMap<String, Integer>();
////        ArrayList<String> portsHashList = new ArrayList<String>();
////
////        for(int remote_port : REMOTE_PORTS ){
////            portsHashList.add(genHash(Integer.toString(remote_port/2)));
////            portsHashValue.put(genHash(Integer.toString(remote_port/2)),remote_port);
////
////        }
////
////        Collections.sort(portsHashList);
////        Collections.reverse(portsHashList);
//
////        for(String portHash:portsHashList){
////            Log.i("hash", String.valueOf(portsHashValue.get(portHash)));
////            Log.i("hash", String.valueOf(genHash(Integer.toString(portsHashValue.get(portHash)/2)).compareTo(genHash("5560")))); // 1
////            portsSortedList.add(portsHashValue.get(portHash));
////        }
//}catch(Exception e){}

        context = this.getContext();
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = (Integer.parseInt(portStr) * 2);

        Log.i("myport",Integer.toString(myPort));

        try {
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
//            Log.e(TAG, "Can't create a ServerSocket");
        }

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "connect");
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {

        MatrixCursor cursor;

        if(selection.contains("@")){
            cursor = (MatrixCursor) fetchLocal();
        }else if(selection.contains("*")){
            cursor = (MatrixCursor) fetchAll();
        }
        else{
            cursor  = (MatrixCursor) fetch(selection);
        }


        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1= MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

//     s1.compare(s2)
//        s1<s2 -> -2
//        s1>s2 -> +2
    public int findMatch(String msg){

        try {
            if(myPort == firstPort){
                if(genHash(msg).compareTo(genHash(Integer.toString(myPort/2))) > 0 && genHash(msg).compareToIgnoreCase(genHash(Integer.toString(rightPort/2))) <= 0){

                    return rightPort;
                }else {
                    return leftPort;
                }
            }

            if(genHash(msg).compareTo(genHash(Integer.toString(myPort/2))) > 0){
                return rightPort;
            }else{
                return leftPort;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        return 0;
    }
//           s1.compare(s2)
//            s1<s2 -> -2
//            s1>s2 -> +2
    private boolean continueOrNot(String msg){
// Needs work
        Log.i("port continueOrNot",Integer.toString(rightPort));
        try{
            if(rightPort == 0){
                return true;
            }

            if(myPort == firstPort){

                if(genHash(msg).compareTo(genHash(Integer.toString(myPort/2))) <= 0 || genHash(msg).compareToIgnoreCase(genHash(Integer.toString(leftPort/2))) > 0){
                    count++;
                    return true;
                }else{
                    return false;



                }
            }

//            genHash(Integer.toString(myPort)).compareToIgnoreCase(genHash(msg)) < 0 && genHash(Integer.toString(myPort+2)).compareToIgnoreCase(genHash(msg)) >0
        if(genHash(msg).compareTo(genHash(Integer.toString(myPort/2))) <= 0 && genHash(msg).compareToIgnoreCase(genHash(Integer.toString(leftPort/2))) > 0){
            count++;
            return true;
        }else{
            return false;
        }

        }catch (Exception e){
            Log.i("Exception", e.getMessage());
        }

        return false;
    }


    public Cursor fetch(String selection){

        MatrixCursor cursor = new MatrixCursor(new String[]{"key","value"});
        String contents = "";
        try {
            String filename = selection + ".txt";
            FileInputStream fis;

            fis = context.openFileInput(filename);
            InputStreamReader inputStreamReader =
                    new InputStreamReader(fis, StandardCharsets.UTF_8);

            BufferedReader reader = new BufferedReader(inputStreamReader);
            String line = reader.readLine();
            while (line != null) {
                contents = contents + line;
                line = reader.readLine();
            }
            cursor.newRow()
                    .add("key", selection)
                    .add("value", contents);

        }catch (FileNotFoundException e){

            try {
                int port = findMatch(selection);
                String value= new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"single",selection,Integer.toString(port)).get();
                cursor.newRow()
                        .add("key", selection)
                        .add("value", value);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            } catch (ExecutionException ex) {
                ex.printStackTrace();
            }

        }
        catch (Exception e){
            e.printStackTrace();
        }


        return cursor;

    }

    public MatrixCursor fetchAll(){

        MatrixCursor globalCursor= new MatrixCursor(new String[]{"key","value"});
        String msg="";
        if(rightPort == 0){
            return fetchLocal();
        }
        try {
            msg = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"queryAll").get();
        }catch (Exception e) {
        }

        String msgs[] = msg.split(":");
        Log.i("cursor length", String.valueOf(msgs.length));
        for(int i=1;i<msgs.length;i = i+2){
            if(msgs[i].equals("null")){continue;}
            globalCursor.newRow()
                    .add("key", msgs[i])
                    .add("value", msgs[i+1]);
            Log.i("cursor key",msgs[i]);
            Log.i("cursor value",msgs[i+1]);
        }
        Log.i("cursor count", String.valueOf(globalCursor.getCount()));
        return globalCursor;
    }

    public MatrixCursor fetchLocal(){

        MatrixCursor cursor = new MatrixCursor(new String[]{"key","value"});


        String[] filenames = context.fileList();
        String contents = "";
        for(String filename : filenames) {
            StringBuilder stringBuilder = new StringBuilder();

            String selection;
            selection = filename.split(".txt")[0];
            try {
                FileInputStream fis;
                fis = context.openFileInput(filename);
                InputStreamReader inputStreamReader =
                        new InputStreamReader(fis, StandardCharsets.UTF_8);

                BufferedReader reader = new BufferedReader(inputStreamReader);
                String line = reader.readLine();
                while (line != null) {
                    stringBuilder.append(line);
                    line = reader.readLine();
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                contents = stringBuilder.toString();
            }
            cursor.newRow()
                    .add("key", selection)
                    .add("value", contents);


        }


        return cursor;

    }
//    /////////////////// Async Tasks Start ////////////////////////////////////////
    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {
            MatrixCursor globalCursor = null;
            if(msgs[0].contains("connect")){

                try {

                    Socket clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 11108);
                    PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
                    BufferedReader dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    pw.println("connect:"+Integer.toString(myPort));

                    String connect = dis.readLine();
                    Log.i("connect",connect);
                    clientSocket.close();

                    ArrayList<String> ports = new ArrayList<String>(Arrays.asList(connect.split(":")));
                    ports.remove(0);


                    Log.i("ports list",ports.toString());
                        String left, right;
                        int index = ports.indexOf(Integer.toString(myPort));

                        Log.i("index",Integer.toString(index));

                         if (index == 0) {
                            right = ports.get(1);
                            left = ports.get(ports.size() - 1);
                        }else if (index == ports.size() - 1) {
                        right = ports.get(0);
                        left = ports.get(index - 1);
                        } else {
                            right = ports.get(index + 1);
                            left = ports.get(index - 1);
                        }
                    rightPort = Integer.parseInt(right);
                    leftPort = Integer.parseInt(left);

                    String msgR;
                    clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(left));
                    pw = new PrintWriter(clientSocket.getOutputStream(), true);
                    dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    pw.println("right:"+Integer.toString(myPort)+":"+ports.get(0));
                    msgR = dis.readLine();
                    if(msgR.contains("ok")){
                        clientSocket.close();
                    }

                    clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(right));
                    pw = new PrintWriter(clientSocket.getOutputStream(), true);
                    dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    pw.println("left:"+Integer.toString(myPort));
                    msgR = dis.readLine();
                    if(msgR.contains("ok")){
                        clientSocket.close();
                    }

                    for(String port : ports){

                        clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                        pw = new PrintWriter(clientSocket.getOutputStream(), true);
                        dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                        pw.println("firstPort:"+ports.get(0));
                        msgR = dis.readLine();
                        if(msgR.contains("ok")){
                            clientSocket.close();
                        }
                    }



                } catch (Exception e) {
                    Log.i("Exception Connectclient",e.getMessage());
                    e.printStackTrace();
                }

            }else if(msgs[0].contains("insert")){
                String key = msgs[1];
                String value = msgs[2];
                String targetPort = msgs[3];

                Socket clientSocket = null;
                try {
                    clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(targetPort));
                    PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
                    BufferedReader dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    pw.println("insert:"+key+":"+value);

                } catch (IOException e) {
                    e.printStackTrace();
                }


            }else  if(msgs[0].contains("single")){
                globalCursor =  new MatrixCursor(new String[]{"key","value"});
                String key = msgs[1];
                String targetPort = msgs[2];
                Socket clientSocket = null;
                try {
                    clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(targetPort));
                    PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);

                    pw.println("single:"+key);
                    pw.flush();
                    BufferedReader dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    String value = dis.readLine();
                    clientSocket.close();
                    return value;

                }catch (Exception e){

                    Log.i("Exception Client",e.getMessage());

                }

            }
            else if(msgs[0].contains("queryAll")){
                globalCursor =  new MatrixCursor(new String[]{"key","value"});
                String msgRec="";
                for(Integer targetPort : REMOTE_PORTS){
                    Socket clientSocket = null;
                    try {
                        clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), targetPort);
                        PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
                        pw.println("queryAll");
                        pw.flush();
                        BufferedReader dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                        clientSocket.setSoTimeout(500);
                        String value = dis.readLine();
                        clientSocket.close();
                        if(value.contains("null")){continue;}
                        msgRec = msgRec + value;

                    } catch (Exception e) {
                        Log.i("Excpetion QueryAll","Excpetion "+ e.getMessage());
                        e.printStackTrace();
                    }

                }
                Log.i("cursor portsList",portsList.toString());
                Log.i("cursor msgRec",Integer.toString(msgRec.split(":").length));
                return msgRec;
            }

            return null;
        }

    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {
                    try {
                        Socket socket = serverSocket.accept();

                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        PrintWriter ds = new PrintWriter(socket.getOutputStream(), true);
                        String msg = in.readLine();
// What if the selected port is 11108 in first round.
                        if(myPort == 11108 && msg.contains("connect")){
                            int clientPort = Integer.parseInt(msg.split(":")[1]);
                            int left,right;
                            if(portsList.size() == 0){
                                portsList.add(clientPort);
                            }else {
                                for(int i=0;i< portsList.size();i++){
                                    if(genHash(Integer.toString(clientPort/2)).compareTo(genHash(Integer.toString(portsList.get(i)/2)))<0){
                                        portsList.add(i,clientPort);
                                        break;
                                    }
                                }
                                if(!portsList.contains(clientPort)){
                                    portsList.add(clientPort);
                                }
                            }
                            String msgToSend = "";
                            for(int port: portsList){
                                msgToSend = msgToSend+":"+Integer.toString(port);
                            }

                            ds.println(msgToSend);
                            ds.flush();
                        }else if(msg.contains("firstPort")){
                            firstPort = Integer.parseInt(msg.split(":")[1]);
                            Log.i("port firstPort",Integer.toString(firstPort));
                            ds.println("ok");
                            ds.flush();
                        }
                        else if(msg.contains("right")){

                            rightPort = Integer.parseInt(msg.split(":")[1]);
                            Log.i("port right",Integer.toString(rightPort));
                            ds.println("ok");
                            ds.flush();
//                            socket.close();
                        }else if(msg.contains("left")){
                            leftPort = Integer.parseInt(msg.split(":")[1]);
                            Log.i("port left",Integer.toString(leftPort));
                            ds.println("ok");
                            ds.flush();
                        }
                        else if(msg.contains("insert")){

                            String key = msg.split(":")[1];
                            String value = msg.split(":")[2];
                            ContentValues contentValues = new ContentValues();
                            contentValues.put("key",key);
                            contentValues.put("value",value);
                            insert(getUri(),contentValues);
                            socket.close();
                        }else if(msg.contains("single")){
                            String key = msg.split(":")[1];
                            MatrixCursor cursor = (MatrixCursor) fetch(key);

                            cursor.moveToFirst();
                                String key1 = cursor.getString(cursor.getColumnIndex("key"));
                                String value = cursor.getString(cursor.getColumnIndex("value"));
                            ds.println(value);

                        }
                        else if(msg.contains("queryAll")){
                            MatrixCursor cursor = (MatrixCursor) fetchLocal();
                            String msgToSend = "";

                            try {
                                cursor.moveToFirst();
                                do {
                                    String key = cursor.getString(cursor.getColumnIndex("key"));
                                    String value = cursor.getString(cursor.getColumnIndex("value"));
                                    msgToSend = msgToSend + ":" + key + ":" + value;

                                } while (cursor.moveToNext());

                                Log.i("cursor msgToSend",msgToSend);

                                if (msgToSend == "") {
                                    msgToSend = ":" + "null" + ":" + "null";
                                }
                                ds.println(msgToSend);

                            }catch (Exception e){
                                Log.i("cursor Exception",msgToSend);
                                ds.println(":" + "null" + ":" + "null");
                            }


                        }

                    } catch (Exception e) {
                        Log.i("Exception Server","Unknown Exception "+e.getMessage());

                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}
