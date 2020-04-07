package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author stevko
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = "PA1";
    public static int msgNumber=1;
    ArrayList<String> REMOTE_PORTS = new ArrayList<String>(Arrays.asList("11108","11112","11116","11120","11124"));
//    String[] REMOTE_PORTS = {"11108","11112","11116","11120","11124"};
//    String[] REMOTE_PORTS = {"11108","11112","11116"};
    HashMap<String, Integer> myPortNum= new HashMap<String, Integer>();
    HashMap<String, Socket> socketMap= new HashMap<String, Socket>();
    HashMap<String,Integer> msgCount = new HashMap<String,Integer>();
    HashMap<String,Double> msgMaxNo = new HashMap<String,Double>();
    String brokeProc = null;
    static final int SERVER_PORT = 10000;
    int myNum = 0;

//    adb -s emulator-5554 uninstall edu.buffalo.cse.cse486586.groupmessenger2
//    adb -s emulator-5556 uninstall edu.buffalo.cse.cse486586.groupmessenger2
//    adb -s emulator-5558 uninstall edu.buffalo.cse.cse486586.groupmessenger2
//    adb -s emulator-5560 uninstall edu.buffalo.cse.cse486586.groupmessenger2
//    adb -s emulator-5562 uninstall edu.buffalo.cse.cse486586.groupmessenger2

    public String myPort="";
    Uri uri;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
        myPortNum.put("11108",1);
        myPortNum.put("11112",2);
        myPortNum.put("11116",3);
        myPortNum.put("11120",4);
        myPortNum.put("11124",5);



        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myNum = myPortNum.get(myPort);
//        Log.i("port",myPort);
//        Log.i("num", Integer.toString(myNum));

        uri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());


        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
//            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                TextView textView = (TextView) findViewById(R.id.editText1);
                String msg = textView.getText().toString();
                textView.setText("");
//                for(String remote:REMOTE_PORTS) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort, "message");

//                }

            }
        });
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while(true) {
                    try {
                        Socket socket = serverSocket.accept();

                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        PrintWriter ds = new PrintWriter(socket.getOutputStream(), true);
                        String msg = in.readLine();
//                        Log.i("newMsg", msg);
                        if (!msg.endsWith("agreed")) {
                            // Attach Port Number to msg number for ISIS Algorithm.
                            double msgFloatNo = msgNumber + (0.1 * myNum);
                            ds.println(msgFloatNo);
                            ds.flush();
                            msgNumber++;
                        }
//                        socket.setSoTimeout(100);
                        // Get messages for second time
                        if ((msg = in.readLine()) != null) {
//                            Log.i("check", "check");
                            if (msg.endsWith("agreed")) {
                                String message = msg.trim().split(":")[0].trim();
                                int key = (int) Double.parseDouble(msg.trim().split(":")[1].trim());
                                ContentValues keyValueToInsert = new ContentValues();
                                keyValueToInsert.put("key", key - 1);
                                keyValueToInsert.put("value", message);
                                Uri newUri = getContentResolver().insert(
                                        uri,
                                        keyValueToInsert
                                );
                            }
                            publishProgress(msg);
                        }

                    }catch(Exception e){
//                        Log.i("Socket Break","Just continue");

                    }





//                    in.close();
//                    ds.close();

//                    socket.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
//                Log.i("serverBreak",e.getMessage());
            }


            return null;
        }

        protected void onProgressUpdate(String...strings) {

            String strReceived = strings[0].trim();
//            Log.i("strReceived", strReceived);



                TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                remoteTextView.append(strReceived + "\n");
//                String msgFloat = strings[0].trim().split(":")[1];
//
//
//                ContentValues keyValueToInsert = new ContentValues();
//
//                keyValueToInsert.put("key",strings[0].trim().split(":")[1]);
//                keyValueToInsert.put("value",strings[0].trim().split(":")[0]);
//                Uri newUri = getContentResolver().insert(
//                        uri,
//                        keyValueToInsert
//                );
            return;
        }

    }




    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String type = msgs[2];
//            try {
                String msgToSend = msgs[0];
                ArrayList<Double> proposedArray = new ArrayList<Double>();
//                if(type.contains("check")){msgToSend = msgToSend+":"+"check";}else{msgToSend = msgToSend+":"+"normal";}
                for (String remotePort : REMOTE_PORTS) {
                    Socket clientSocket = null;
                    try {
                        clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                    try {

                        socketMap.put(remotePort, clientSocket);
                        PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
                        BufferedReader dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

//                        Log.i("working", "working");
                        pw.println(msgToSend);
                        pw.flush();
                        String proposedSeqNo = dis.readLine();
//                        Log.i("newcom", proposedSeqNo);
                        proposedArray.add(Double.parseDouble(proposedSeqNo));
                    }catch (Exception e){
                        try {
                            clientSocket.close();
                        } catch (IOException ex) {
                            ex.printStackTrace();
                        }
//                        Log.i("clientBreak1",e.getMessage());
                        brokeProc = remotePort;
                    }
                }

                if(brokeProc!=null){
                    if(REMOTE_PORTS.contains(brokeProc)){REMOTE_PORTS.remove(brokeProc);}
//                    Log.i("clientBreak1",brokeProc);
                }

                double agreedNum = Collections.max(proposedArray);
//                Log.i("agreedNum",Double.toString(agreedNum));
                msgToSend = msgToSend+":"+Double.toString(agreedNum)+":"+"agreed";
                for(String remotePort : REMOTE_PORTS){
                    Socket socket = null;
                    try {
                        socket = socketMap.get(remotePort);
                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                        pw.println(msgToSend);
//                    socket.close();
                    }catch (Exception e){
//                        Log.i("clientBreak2",e.getMessage());
                        try {
                            socket.close();
                        } catch (IOException ex) {
                            ex.printStackTrace();
                        }
//                        Log.i("clientBreak1",e.getMessage());
                        brokeProc = remotePort;

                    }
                }
            if(brokeProc!=null){
                if(REMOTE_PORTS.contains(brokeProc)){REMOTE_PORTS.remove(brokeProc);}
//                Log.i("clientBreak1",brokeProc);
            }
                socketMap.clear();

//            } catch (UnknownHostException e) {
//                Log.e(TAG, "ClientTask UnknownHostException");
//            } catch (IOException e) {
//                Log.e(TAG, "ClientTask socket IOException");
//            }
//            catch(Exception e){
//                e.printStackTrace();
//            }

            return null;
        }
    }
}
