package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.provider.MediaStore;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.util.Log;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


// command to test
// ./simpledht-grading.linux /home/vamsi/AndroidStudioProjects/SimpleDht/app/build/outputs/apk/debug/app-debug.apk

public class SimpleDhtProvider extends ContentProvider {
    Context context;
    int myPort,leftPort=0,rightPort=0;
    int count = 0;
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
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"insert", values.get("key").toString(),values.get("value").toString(),Integer.toString(targetPort));
                return  uri;
            }



            Log.i("path insert key",values.get("key").toString());
            Log.i("path insert value",values.get("value").toString());
            String filename = values.get("key").toString()+".txt";
            Log.i("file","writing to a file "+filename);
            String msg = values.get("value").toString();

            File file = new File(context.getFilesDir(), filename);

            Log.i("msg",context.getFilesDir().toString());

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
        Log.i("count",Integer.toString(count));
        Log.i("port left",Integer.toString(leftPort));
        Log.i("port right",Integer.toString(rightPort));
        MatrixCursor cursor;

//        if(!continueOrNot(selection)){
//            Log.i("return",selection);
//            return  null;
//        }

        Log.i("path query",selection);
        if(selection.contains("*") || selection.contains("@")){
            cursor = (MatrixCursor) fetchall();
        }else{
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

//    s1.compareTo(s2)
//    12 if s1 < s2
//    -16 if s1 > s2
    public int findMatch(String msg){

        try {
            if(myPort == 11108){
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

    private boolean continueOrNot(String msg){
// Needs work
        try{
            if(rightPort == 0){
                return true;
            }

            if(myPort == 11108){
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

        Log.i("query",selection);


        StringBuilder stringBuilder = new StringBuilder();
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
                stringBuilder.append(line);
                line = reader.readLine();
            }

        }catch (Exception e){
            e.printStackTrace();
        } finally {
            contents = stringBuilder.toString();
        }

        Log.i("fileContents",contents);
        cursor.newRow()
                .add("key", selection)
                .add("value", contents);


        if(contents==""){return null;}

        return cursor;

    }

    public Cursor fetchall(){

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

            Log.i("path fetch key",selection);
            Log.i("path fetch value",contents);
            cursor.newRow()
                    .add("key", selection)
                    .add("value", contents);

        }


        if(contents==""){return null;}

        return cursor;

    }


//    /////////////////// Async Tasks Start ////////////////////////////////////////

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            if(msgs[0].contains("connect")){

                try {

                    Socket clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 11108);
                    PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
                    BufferedReader dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    pw.println("connect:"+Integer.toString(myPort));
                    String left_right = dis.readLine();
                    leftPort = Integer.parseInt(left_right.split(":")[0]);
                    rightPort = Integer.parseInt(left_right.split(":")[1]);

                    Log.i("Target ports",Integer.toString(leftPort));
                    Log.i("Target my ports",Integer.toString(myPort));
                    Log.i("Target ports",Integer.toString(rightPort));

                } catch (Exception e) {
                    leftPort = 0;
                    rightPort = 0;
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
                            int left = clientPort - 4;int right = clientPort+4;
                            if(clientPort == 11108){left = 11124;}
                            else if(clientPort == 11124){right = 11108;}
                            String msgToSend = Integer.toString(left)+":"+Integer.toString(right);
                            ds.println(msgToSend);
                        }else if(msg.contains("insert")){

                            String key = msg.split(":")[1];
                            String value = msg.split(":")[2];
                            ContentValues contentValues = new ContentValues();
                            contentValues.put("key",key);
                            contentValues.put("value",value);
                            insert(getUri(),contentValues);

                        }








                    } catch (Exception e) {

                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}
