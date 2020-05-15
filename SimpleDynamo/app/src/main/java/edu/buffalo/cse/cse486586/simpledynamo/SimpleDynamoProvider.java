package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;

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
//
// ./simpledynamo-grading.linux /home/vamsi/AndroidStudioProjects/SimpleDynamo/app/build/outputs/apk/debug/app-debug.apk -p 1



public class SimpleDynamoProvider extends ContentProvider {
	Context context;
	int myPort,leftPort=0,rightPort=0;
	int count = 0;
	int firstPort = 11108;
	ArrayList<Integer> REMOTE_PORTS = new ArrayList<Integer>(Arrays.asList(11108,11112,11116,11120,11124));
	ArrayList<Integer> portsSortedList = new ArrayList<Integer>();

	int myPortpos = 0;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		String contents = "";
		try {

			if(selection.equals("@")){

				String[] filenames = context.fileList();
				for(String filename: filenames){
					context.deleteFile(filename);
				}

			}else if(selection.equals("*")){
				try {
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"deleteAll");
				} catch (Exception ex){

				}

			}else{
				String filename = selection + ".txt";
				FileInputStream fis;
				if(!context.deleteFile(filename)){
					try {
						int port = findMatch(selection);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"deleteOne",selection,Integer.toString(port));
					} catch (Exception ex){
					}
				}
			}



		}catch (Exception e){
		}
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
		int replicationCount = 0;
		try {

			if(values.containsKey("replication")){
				replicationCount = Integer.parseInt(values.get("replication").toString());
			}
			else if(!continueOrNot(values.get("key").toString())){
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

		if(replicationCount == 0){
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"replication", values.get("key").toString(),values.get("value").toString(),"2");
		}else if(replicationCount == 2) {
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"replication", values.get("key").toString(),values.get("value").toString(),"1");
		}


//

		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		context = this.getContext();
		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = (Integer.parseInt(portStr) * 2);
		try {
			Log.i("basic check", String.valueOf(genHash("5554").compareTo(genHash("5556")))); // 1
			Log.i("basic check", String.valueOf(genHash("5556").compareTo(genHash("5558")))); // -47
			Log.i("basic check", String.valueOf(genHash("5558").compareTo(genHash("5560")))); // -2
			Log.i("basic check", String.valueOf(genHash("5560").compareTo(genHash("5562")))); // 50

			HashMap<String,Integer> portsHashValue = new HashMap<String, Integer>();
			ArrayList<String> portsHashList = new ArrayList<String>();

			for(int remote_port : REMOTE_PORTS ){
				portsHashList.add(genHash(Integer.toString(remote_port/2)));
				portsHashValue.put(genHash(Integer.toString(remote_port/2)),remote_port);

			}

			Collections.sort(portsHashList);
//        Collections.reverse(portsHashList);

			for(String portHash:portsHashList){
				Log.i("hash",Integer.toString(portsHashValue.get(portHash)));
				Log.i("hash", String.valueOf(genHash(Integer.toString(portsHashValue.get(portHash)/2)).compareTo(genHash("5560")))); // 1
				portsSortedList.add(portsHashValue.get(portHash));
			}

			myPortpos = 0;
			for(int port:portsSortedList){
				if(port == myPort){
					break;
				}else{
					myPortpos++;
				}
			}

		}catch(Exception e){}

		Log.i("portsSortedList", portsSortedList.toString());



		Log.i("myport",Integer.toString(myPort));
		Log.i("myPortpos",Integer.toString(myPortpos));

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

		if(selection.contains("@")){
			cursor = (MatrixCursor) fetchLocal();
		}else if(selection.contains("*")){
			cursor = (MatrixCursor) fetchAll();
		}
		else{
			cursor  = (MatrixCursor) fetch(selection);
		}
//        if(selection.contains("@")){
//            cursor = (MatrixCursor) fetchLocal();
//        }else if(selection.contains("*")){
//            cursor = (MatrixCursor) fetchAll();
//        }
//        else{
//            cursor  = (MatrixCursor) fetch(selection);
//        }

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
			if(myPort == 11124){
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
		try{
			if(rightPort == 0){
				return true;
			}

			if(myPort == 11124){

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

			Log.i("fileContents",contents);
			cursor.newRow()
					.add("key", selection)
					.add("value", contents);

		}catch (FileNotFoundException e){

			try {
				int port = findMatch(selection);
				String value= new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"single",selection,Integer.toString(port)).get();
				Log.i("fetch value",value);
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

		Log.i("length",Integer.toString(msgs.length));
		for(int i=1;i<msgs.length;i = i+2){
			Log.i("length key",msgs[i]);
			Log.i("length value",msgs[i+1]);
			globalCursor.newRow()
					.add("key", msgs[i])
					.add("value", msgs[i+1]);
		}

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

			Log.i("path fetch key",selection);
			Log.i("path fetch value",contents);

//            testPosition(selection);

			cursor.newRow()
					.add("key", selection)
					.add("value", contents);


		}

		if(contents==""){return null;}

		return cursor;

	}

	public void testPosition(String key)  {
		try {
			Log.i("testPosition key",key);
			Log.i("testPosition myPort", String.valueOf(genHash(key).compareTo(genHash(Integer.toString(myPort / 2)))));
			Log.i("testPosition leftPort", String.valueOf(genHash(key).compareTo(genHash(Integer.toString(leftPort / 2)))));

			Log.i(key,String.valueOf(genHash(key).compareTo(genHash(Integer.toString(5554)))));
			Log.i(key,String.valueOf(genHash(key).compareTo(genHash(Integer.toString(5556)))));
			Log.i(key,String.valueOf(genHash(key).compareTo(genHash(Integer.toString(5558)))));
			Log.i(key,String.valueOf(genHash(key).compareTo(genHash(Integer.toString(5560)))));
			Log.i(key,String.valueOf(genHash(key).compareTo(genHash(Integer.toString(5562)))));



		}catch (Exception e){
			Log.i("testPosition Exception","Exception");
		}
	}

//    /////////////////// Async Tasks Start ////////////////////////////////////////

	public static class MartixCursorSerialized implements Serializable{

		MatrixCursor matrixCursor;

		public MartixCursorSerialized(MatrixCursor cursor) {
			this.matrixCursor = cursor;
		}


		public MatrixCursor getMatrixCursor() {
			return matrixCursor;
		}
	}

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


			}else  if(msgs[0].contains("replication")){
				String key = msgs[1];
				String value = msgs[2];
				int replicationCount  = Integer.parseInt(msgs[3]);
				int targetPort = portsSortedList.get((myPortpos+1)%5);
				try {
					Socket clientSocket = null;
					clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), targetPort);
					PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
					BufferedReader dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					Log.i("replication","replication:"+key+":"+value+":"+Integer.toString(replicationCount)+"targetPort:"+Integer.toString(targetPort));
					pw.println("replication:"+key+":"+value+":"+Integer.toString(replicationCount));

				} catch (IOException e) {
					e.printStackTrace();
				}


			}
			else  if(msgs[0].contains("single")){
				globalCursor =  new MatrixCursor(new String[]{"key","value"});
				String key = msgs[1];
				String targetPort = msgs[2];
				Log.i("single",key+" "+targetPort);
				Socket clientSocket = null;
				try {
					clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(targetPort));
					PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);

					pw.println("single:"+key);
					pw.flush();
					BufferedReader dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					String value = dis.readLine();
					Log.i("Client key",key);
					Log.i("Client value",value);
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
						String value = dis.readLine();
						clientSocket.close();
						msgRec = msgRec + value;
					} catch (Exception e) {
						Log.i("Excpetion QueryAll", e.getMessage());
						e.printStackTrace();
					}

				}
				return msgRec;
			}else if(msgs[0].contains("deleteOne")){

				try {
					String key = msgs[1];
					String targetPort = msgs[2];
					Socket clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(targetPort));
					PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
					BufferedReader dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					pw.println("deleteOne:"+key);

				}catch (Exception e){
					Log.i("Exception delete","delete "+e.getMessage());
				}

			}else if(msgs[0].contains("deleteAll")){
				for(Integer targetPort : REMOTE_PORTS){
					Socket clientSocket = null;
					try {
						clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), targetPort);
						PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
						pw.println("deleteAll");
						pw.flush();
						clientSocket.setSoTimeout(500);
					} catch (Exception e) {
						Log.i("Excpetion deleteAll","Excpetion "+ e.getMessage());
						e.printStackTrace();
					}

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
						Log.i("server msg",msg);
// What if the selected port is 11108 in first round.
						if(myPort == 11108 && msg.contains("connect")){
							int clientPort = Integer.parseInt(msg.split(":")[1]);
							int left,right;
							int index = portsSortedList.indexOf(clientPort);

							if(index == portsSortedList.size()-1){
								right = portsSortedList.get(0);
								left = portsSortedList.get(index-1);
							}else if(index == 0){
								right = portsSortedList.get(1);
								left = portsSortedList.get(portsSortedList.size()-1);
							}else{
								right = portsSortedList.get(index+1);
								left = portsSortedList.get(index-1);
							}
							String msgToSend = Integer.toString(left)+":"+Integer.toString(right);
							ds.println(msgToSend);
						}else if(msg.contains("insert")){

							String key = msg.split(":")[1];
							String value = msg.split(":")[2];
							ContentValues contentValues = new ContentValues();
							contentValues.put("key",key);
							contentValues.put("value",value);
							insert(getUri(),contentValues);
                            socket.close();
						}else if(msg.contains("replication")){

							String key = msg.split(":")[1];
							String value = msg.split(":")[2];
							String replicationCount = msg.split(":")[3];
							ContentValues contentValues = new ContentValues();
							contentValues.put("key",key);
							contentValues.put("value",value);
							contentValues.put("replication",replicationCount);
							insert(getUri(),contentValues);
							socket.close();
						}
						else if(msg.contains("single")){
							String key = msg.split(":")[1];
							Log.i("single server",key);
							MatrixCursor cursor = (MatrixCursor) fetch(key);

							cursor.moveToFirst();
							String key1 = cursor.getString(cursor.getColumnIndex("key"));
							String value = cursor.getString(cursor.getColumnIndex("value"));
							Log.i("cursor key",key1);
							Log.i("cursor value",value);
							ds.println(value);

						}
						else if(msg.contains("queryAll")){
							MatrixCursor cursor = (MatrixCursor) fetchLocal();
							String msgToSend = "";

							Log.i("length count",Integer.toString(cursor.getCount()));

							cursor.moveToFirst();
							do{
								String key = cursor.getString(cursor.getColumnIndex("key"));
								String value = cursor.getString(cursor.getColumnIndex("value"));
								msgToSend = msgToSend+":"+key+":"+value;
								Log.i("queryAll cursor",key);

							}while(cursor.moveToNext());

							ds.println(msgToSend);

						}else if(msg.contains("deleteAll")){
							delete(getUri(),"@",null);
						}
						else if(msg.contains("deleteOne")){
							String key = msg.split(":")[1];
							delete(getUri(),key,null);

						}

					} catch (Exception e) {
						Log.i("Exception Server",e.getMessage());
					}

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}
	}
}
