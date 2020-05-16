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
	int failedPort = 0;
	int recoveredPort = 0;
	ArrayList<Integer> REMOTE_PORTS = new ArrayList<Integer>(Arrays.asList(11108,11112,11116,11120,11124));
	ArrayList<Integer> portsSortedList = new ArrayList<Integer>();

	int myPortpos = 0;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		String contents = "";
		try {

			if(selection.equals("trim")){
				int b1 = leftPort;
				int l1 = getLeftPort(b1);
				int b2 = l1;
				int l2 = getLeftPort(b2);

				String[] filenames = context.fileList();
				for(String filename: filenames){
					String msg = filename.split(".txt")[0];
					if(continueOrNot(msg,myPort,leftPort)){
						continue;
					}else if(continueOrNot(msg,b1,l1)){
						continue;
					}else if(continueOrNot(msg,b2,l2)){
						continue;
					}else{
						context.deleteFile(filename);
						Log.i("delete","delete");
					}

				}


			}
			else if(selection.equals("@")){

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


		if(values.containsKey("fetchAll")){
//			int b0 = myPort;
//			int l0 = getLeftPort(myPort);
			int b1 = leftPort;
			int l1 = getLeftPort(b1);
			int b2 = l1;
			int l2 = getLeftPort(b2);


			Log.i("connectback","fetchAll");
			String msg = values.get("value").toString();
			String msgs[] = msg.split(":");
			Log.i("connectback","count"+Integer.toString(msgs.length));

			for(int i=1;i<msgs.length;i = i+2){
				Log.i("length key",msgs[i]);
				Log.i("length value",msgs[i+1]);
				String key = msgs[i];
				int replicationCount = 0;
				if(continueOrNot(key,myPort,leftPort)){
					replicationCount = 2;
				}else if(continueOrNot(key,b1,l1)){
					replicationCount = 1;
				}else if (continueOrNot(key,b2,l2)){
					replicationCount = 0;
				}else{
					continue;
				}

				try{
					String filename = key + ".txt";
					String val = msgs[i+1];

					File file = new File(context.getFilesDir(), filename);

					FileOutputStream fos = context.openFileOutput(filename, Context.MODE_PRIVATE);
					fos.write(val.getBytes());
					fos.close();
					if(replicationCount!=0){
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "replication", key, val, Integer.toString(replicationCount));
					}

				} catch (Exception e) {
					e.printStackTrace();
					Log.i("file", "can not write to file");
				}



			}

		}else {


			boolean replication = true;
			try {

				int targetPort = findMatch(values.get("key").toString());
				if (values.containsKey("replication")) {
					replication = false;
				} else if( values.containsKey("store")){
					replication = false;
				}else if (targetPort != myPort) {
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", values.get("key").toString(), values.get("value").toString(), Integer.toString(targetPort));
					return uri;
				}

				Log.i("inserted", values.get("key").toString());
				String filename = values.get("key").toString() + ".txt";
				String msg = values.get("value").toString();

				File file = new File(context.getFilesDir(), filename);

				FileOutputStream fos = context.openFileOutput(filename, Context.MODE_PRIVATE);
				fos.write(msg.getBytes());
				fos.close();
				if(replication){
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "replication", values.get("key").toString(), values.get("value").toString());
				}

			} catch (Exception e) {
				e.printStackTrace();
				Log.i("file", "can not write to file");
			}

//			if (replicationCount == 0) {
//				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "replication", values.get("key").toString(), values.get("value").toString(), "2");
//			} else if (replicationCount == 2) {
//				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "replication", values.get("key").toString(), values.get("value").toString(), "1");
//			}


//
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
		try {
			Log.i("basic check", String.valueOf(genHash("5554").compareTo(genHash("5556")))); // 1
			Log.i("basic check", String.valueOf(genHash("5556").compareTo(genHash("5558")))); // -47
			Log.i("basic check", String.valueOf(genHash("5558").compareTo(genHash("5560")))); // -2
			Log.i("basic check", String.valueOf(genHash("5560").compareTo(genHash("5562")))); // 50


			loadPorts();
			loadPortPos();

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


	public void loadPortPos(){
		myPortpos = 0;
		for(int port:portsSortedList){
			if(port == myPort){
				break;
			}else{
				myPortpos++;
			}
		}
	}


	public void loadPorts(){
try {
		REMOTE_PORTS = new ArrayList<Integer>(Arrays.asList(11108,11112,11116,11120,11124));
	    portsSortedList.clear();
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
		int index = portsSortedList.indexOf(myPort);

		if(index == 0){
			leftPort = portsSortedList.get(portsSortedList.size()-1);
			rightPort = portsSortedList.get(index+1);
		}else if(index == portsSortedList.size()-1){
			leftPort = portsSortedList.get(index-1);
			rightPort = portsSortedList.get(0);
		}else {
			leftPort = portsSortedList.get(index-1);
			rightPort = portsSortedList.get(index+1);
		}


}catch (Exception e){}

	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {

		MatrixCursor cursor;
		Log.i("path query",selection);

		if(selection.contains("@")){
			cursor = (MatrixCursor) fetchLocal();
		}else if(selection.contains("*")){
			cursor = (MatrixCursor) fetchAll();
		}else if(selection.contains("original")){
			cursor = (MatrixCursor) fetchOriginal();
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

			for(int i=1;i<portsSortedList.size();i++){
				if(genHash(msg).compareTo(genHash(Integer.toString(portsSortedList.get(i)/2))) < 0 && genHash(msg).compareTo(genHash(Integer.toString(portsSortedList.get(i-1)/2))) > 0){
					return portsSortedList.get(i);
				}
			}

			return portsSortedList.get(0);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}


		return 0;
	}
	//           s1.compare(s2)
//            s1<s2 -> -2
//            s1>s2 -> +2

	private int getLeftPort(int current){

		if(current == portsSortedList.get(0)){
			return portsSortedList.get(portsSortedList.size()-1);
		}
		int index = portsSortedList.indexOf(Integer.valueOf(current));
		return portsSortedList.get(index-1);
	}

	private int getRightPort(int current){

		if(current ==  portsSortedList.get(portsSortedList.size()-1)){
			return portsSortedList.get(0);
		}
		int index = portsSortedList.indexOf(Integer.valueOf(current));
		return portsSortedList.get(index+1);
	}

	private boolean continueOrNot(String msg,int current,int left){
// Needs work
		try{

			if(current == portsSortedList.get(0)){

				if(genHash(msg).compareTo(genHash(Integer.toString(current/2))) <= 0 || genHash(msg).compareToIgnoreCase(genHash(Integer.toString(left/2))) > 0){

					return true;
				}else{
					return false;
				}
			}

			if(genHash(msg).compareTo(genHash(Integer.toString(current/2))) <= 0 && genHash(msg).compareToIgnoreCase(genHash(Integer.toString(left/2))) > 0){

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

		Log.i("connectback","fetchAll function");

		MatrixCursor globalCursor= new MatrixCursor(new String[]{"key","value"});
		String msg="";
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


	public MatrixCursor fetchOriginal(){

		MatrixCursor cursor = new MatrixCursor(new String[]{"key","value"});


		String[] filenames = context.fileList();
		String contents = "";
		for(String filename : filenames) {
			String selection;
			selection = filename.split(".txt")[0];

//			if(!continueOrNot(selection)){
//				continue;
//			}


			StringBuilder stringBuilder = new StringBuilder();
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

				for(int port:REMOTE_PORTS) {
					if(port==myPort){continue;}
					try {
						Socket clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
						PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
						BufferedReader dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
						clientSocket.setSoTimeout(100);
						pw.println("connect:" + Integer.toString(myPort));
						String res = dis.readLine();
						clientSocket.close();
						if(res.contains("connectback")){
							recoveredPort = myPort;
							break;
						}
						Log.i("connect","connect "+res);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				if(recoveredPort == myPort){
					failedPort = 0;
					recoveredPort = 0;
					Log.i("connectback","connectback");

					String msgRec="";
					for(Integer targetPort : REMOTE_PORTS){
						if(targetPort == myPort){continue;}
						Socket clientSocket = null;
						try {
							clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), targetPort);
							PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
							pw.println("queryTrim");
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
					ContentValues contentValues = new ContentValues();
					contentValues.put("fetchAll","fetchAll");
					contentValues.put("value",msgRec);
					insert(getUri(),contentValues);



				}
			}
			else if(msgs[0].contains("insert")){
				String key = msgs[1];
				String value = msgs[2];
				String targetPort = msgs[3];

				Socket clientSocket = null;
				Log.i("insertClientTask",targetPort);
				try {
					clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(targetPort));
					PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
					BufferedReader dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					clientSocket.setSoTimeout(100);
					pw.println("insert:"+key+":"+value);
					String res = dis.readLine();
					clientSocket.close();
					Log.i("dis","dis:"+targetPort+":"+res);
					if(res == null){
						failedPort = Integer.parseInt(targetPort);
						ContentValues contentValues = new ContentValues();
						contentValues.put("key",key);
						contentValues.put("value",value);
						contentValues.put("store","store");
						insert(getUri(),contentValues);

//						try{
//
//							clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), getRightPort(failedPort));
//							pw = new PrintWriter(clientSocket.getOutputStream(), true);
//							dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//							pw.println("replication:"+key+":"+value+":2");
//							res = dis.readLine();
//							clientSocket.close();
//
//						}catch (Exception e){}

					}
				} catch (Exception e) {
					Log.i("clientException","socket timeout "+targetPort);

				}


			}else  if(msgs[0].contains("replication")){
				String key = msgs[1];
				String value = msgs[2];
				ArrayList<Integer> targetPorts = new ArrayList<Integer>();
				targetPorts.add(getRightPort(myPort));
				targetPorts.add(getRightPort(targetPorts.get(0)));

				for(int targetPort:targetPorts){
					try {
						Socket clientSocket = null;
						clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), targetPort);
						PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
						BufferedReader dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
						pw.println("replication:"+key+":"+value);
						String res = dis.readLine();
						clientSocket.close();

					} catch (IOException e) {
						e.printStackTrace();
					}
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

						if(msg.contains("connect")){
							int port = Integer.parseInt(msg.split(":")[1]);

							Log.i("connect failedport",Integer.toString(failedPort));

							if(failedPort != port){
								ds.println("normal");
							}else{
								failedPort = 0;
//								recoveredPort = Integer.parseInt(msg.split(":")[1]);
//								loadPorts();
//								loadPortPos();
								ds.println("connectback");
							}


						}else if(msg.contains("insert")){

							String key = msg.split(":")[1];
							String value = msg.split(":")[2];
							ContentValues contentValues = new ContentValues();
							contentValues.put("key",key);
							contentValues.put("value",value);
							ds.println("done");
							insert(getUri(),contentValues);
						}else if(msg.contains("replication")){

							String key = msg.split(":")[1];
							String value = msg.split(":")[2];
							ContentValues contentValues = new ContentValues();
							contentValues.put("key",key);
							contentValues.put("value",value);
							contentValues.put("replication","replication");
							ds.println("done");
							insert(getUri(),contentValues);
//							socket.close();
						}else if(msg.contains("failedPort")){
							failedPort = Integer.parseInt(msg.split(":")[1]);
							Log.i("serverFailedPort",Integer.toString(failedPort));
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
						else if(msg.contains("rearrange")){
							ds.println("done");
							recoveredPort = Integer.parseInt(msg.split(":")[1]);
							failedPort = 0;
							loadPorts();
							loadPortPos();
							Log.i("recoveredPort",Integer.toString(recoveredPort));
							Log.i("recoveredPort right",Integer.toString(rightPort));
							if(rightPort!=recoveredPort){
								delete(getUri(),"trim",null);
							}

						} else if(msg.contains("queryTrim")){

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

							delete(getUri(),"trim",null);

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
