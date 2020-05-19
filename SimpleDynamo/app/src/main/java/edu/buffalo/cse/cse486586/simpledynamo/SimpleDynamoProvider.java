package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import java.text.SimpleDateFormat;
import java.util.Date;

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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


// command to test
//
// ./simpledynamo-grading.linux /home/vamsi/AndroidStudioProjects/SimpleDynamo/app/build/outputs/apk/debug/app-debug.apk -p 1



public class SimpleDynamoProvider extends ContentProvider {
	Context context;
	int myPort,leftPort=0,rightPort=0;
	int failedPort = 0;
	ArrayList<Integer> REMOTE_PORTS = new ArrayList<Integer>(Arrays.asList(11108,11112,11116,11120,11124));
	ArrayList<Integer> portsSortedList = new ArrayList<Integer>();
	int myPortpos = 0;
	Date minDate;
	SimpleDateFormat formatter = new SimpleDateFormat("MM-dd-HH-mm-ss");

//	ReadWriteLock lock = new ReentrantReadWriteLock();
//	Lock writeLock = lock.writeLock();
//	Lock readLock = lock.readLock();

	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
//		writeLock.lock();
		String contents = "";
		try {

//			int targetPort = findMatch(selection);

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

			}else if(selection.contains("direct")){
				String msg = selection.split(":")[0];
				String targetPort = Integer.toString(findMatch(msg));
				String filename =  msg+ ".txt";

				File mydir = context.getDir(targetPort, Context.MODE_PRIVATE);
				File file = new File(mydir, filename);
				file.delete();
			}
			else{
				int targetPort = findMatch(selection);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"deleteOne",selection,Integer.toString(targetPort));

			}



		}catch (Exception e){
		}
//		writeLock.unlock();
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
	public synchronized Uri insert(Uri uri, ContentValues values) {

		if(values.containsKey("fetchAll")){
			String dir =  values.get("fetchAll").toString();

			String msg = values.get("value").toString();
			String msgs[] = msg.split(":");
//			writeLock.lock();
			for(int i=1;i<msgs.length;i = i+2) {
				String key = msgs[i];
				try {
					String filename = key + ".txt";
					String val = msgs[i + 1];
					File mydir = context.getDir(dir, Context.MODE_PRIVATE);
					File file = new File(mydir, filename);
					file.createNewFile();
					FileOutputStream fos = new FileOutputStream(file);
					fos.write(val.getBytes());
					fos.close();

				} catch (IOException ee) {
					ee.printStackTrace();
				}
			}
//			writeLock.unlock();
		}
		else {

			String dir = Integer.toString(myPort);
			boolean replication = true;
			try {

				if (values.containsKey("replication")) {
					replication = false;
					dir = values.get("replication").toString();
				}else if (values.containsKey("store")) {
					replication = false;
				}
				else if (findMatch(values.get("key").toString()) != myPort) {
					int targetPort = findMatch(values.get("key").toString());
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", values.get("key").toString(), values.get("value").toString(), Integer.toString(targetPort));
					return uri;
				}

				if(replication){
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "replication", values.get("key").toString(), values.get("value").toString(),Integer.toString(myPort));
				}

//				writeLock.lock();

				Log.i("inserted "+replication, values.get("key").toString());
				String filename = values.get("key").toString() + ".txt";
				String msg = values.get("value").toString();

				if(msg.contains(",")){
					String original = msg.split(",")[0];
					msg = original+","+getDate();
				}else{
					msg = msg+","+getDate();
				}

				File mydir = context.getDir(dir, Context.MODE_PRIVATE);
				File file = new File(mydir, filename);
				file.createNewFile();
				FileOutputStream fos = new FileOutputStream(file);
				fos.write(msg.getBytes());
				fos.close();

//				writeLock.unlock();

			} catch (Exception e) {
				e.printStackTrace();
				Log.i("file", "can not write to file");
			}


//
		}
		return uri;
	}

	public String getDate(){
		Date date = new Date();
		return formatter.format(date);
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		ReadWriteLock lock = new ReentrantReadWriteLock();
		Lock writeLock = lock.writeLock();
		Lock readLock = lock.readLock();


		String sDate1="01-01-01-01-01";
		Date old= null;
		try {
			old = formatter.parse(sDate1);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		System.out.println(sDate1+"\t"+old);

		Date date = new Date();
		minDate = old;
		System.out.println("datenow "+formatter.format(date));
		System.out.println("datenow past "+formatter.format(old));


		if(date.after(old)){
			System.out.println("datenow "+"fine");
		}
		if(old.after(date)){
			System.out.println("datenow "+"bad");
		}
		context = this.getContext();
		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = (Integer.parseInt(portStr) * 2);
		try {
			loadPorts();
			loadPortPos();
		}catch(Exception e){}



		try {
			ServerSocket serverSocket = new ServerSocket(10000);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
//            Log.e(TAG, "Can't create a ServerSocket");
		}
		Log.i("finalcheck",portsSortedList.toString());
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
//		try {
//			Thread.sleep(250);
//		}catch (Exception ee){}
		MatrixCursor cursor;
		Log.i("path query",selection);

		if(selection.contains("@")){
			cursor = (MatrixCursor) fetchLocal(false,REMOTE_PORTS);
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

	public synchronized Cursor fetchFile(String selection) {

//		try {
//			Thread.sleep(100);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}

//		readLock.lock();
		MatrixCursor cursor = new MatrixCursor(new String[]{"key","value"});

		Log.i("query",selection);

		int pport = findMatch(selection);
		File mydir = context.getDir(Integer.toString(pport), Context.MODE_PRIVATE);

		String contents = "";
		try {
			String filename = selection + ".txt";
			File file = new File(mydir,filename);
			FileInputStream fis = new FileInputStream(file);

//			fis = context.openFileInput(filename);
			InputStreamReader inputStreamReader =
					new InputStreamReader(fis, StandardCharsets.UTF_8);

			BufferedReader reader = new BufferedReader(inputStreamReader);
			String line = reader.readLine();
			while (line != null) {
				contents = contents + line;
				line = reader.readLine();
			}

			Log.i("fileContents fetchfile",contents);
			cursor.newRow()
					.add("key", selection)
					.add("value", contents);

		}catch (FileNotFoundException e){
//			readLock.unlock();
			return null;
		}
		catch (Exception e){
			e.printStackTrace();
		}

//		readLock.unlock();
		return cursor;


	}

	public Cursor fetch(String selection){

//				try {
//			Thread.sleep(250);
//		}catch (Exception ee){}

		MatrixCursor cursor = new MatrixCursor(new String[]{"key","value"});

		Log.i("query",selection);

		String contents = "";

		try {
			int port = findMatch(selection);
			Log.i("port",selection+":"+Integer.toString(port));
			String value= new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"single",selection,Integer.toString(port)).get();
			Log.i("finalcheck",selection+":targetPort:"+Integer.toString(port)+" :myPort:"+Integer.toString(myPort)+" :R1:"+Integer.toString(getRightPort(port))+" :R2:"+Integer.toString(getRightPort(getRightPort(port))));

//				if(value.contains("initial")){
//					return fetch(selection);
//				}
			if(value.contains(":")){
				value = value.split(":")[0];
			}

			cursor.newRow()
					.add("key", selection)
					.add("value", value);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		} catch (ExecutionException ex) {
			ex.printStackTrace();
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

		for(int i=1;i<msgs.length;i = i+2){

			if(msgs[i+1].contains(",")){
				msgs[i+1] = msgs[i+1].split(",")[0];
			}

			globalCursor.newRow()
					.add("key", msgs[i])
					.add("value", msgs[i+1]);
		}

		return globalCursor;
	}
	public MatrixCursor fetchLocal(boolean version, ArrayList<Integer> targetPorts){

		MatrixCursor cursor = new MatrixCursor(new String[]{"key","value"});

		for(int pport:targetPorts) {

			File mydir = context.getDir(Integer.toString(pport), Context.MODE_PRIVATE);
			File[] files = mydir.listFiles();
			Log.i("fetchLocal", Integer.toString(myPort) + " " + Integer.toString(files.length));
			if (files.length == 0) {
				continue;
			}
			String contents = "";
			for (File file : files) {
				StringBuilder stringBuilder = new StringBuilder();

				String selection;
				selection = file.getName().split(".txt")[0];
				try {
					FileInputStream fis = new FileInputStream(file);
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
//            testPosition(selection);
				if (contents.contains(",") && !version) {
					contents = contents.split(",")[0];
				}
				cursor.newRow()
						.add("key", selection)
						.add("value", contents);

			}

		}
//		if(contents==""){return null;}

		return cursor;

	}


//    /////////////////// Async Tasks Start ////////////////////////////////////////

	private class ClientTask extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... msgs) {
			MatrixCursor globalCursor = null;
			if(msgs[0].contains("connect")){

				ArrayList<Integer> tempPorts = new ArrayList<Integer>();
				tempPorts.add(getLeftPort(myPort));
				tempPorts.add(getLeftPort(tempPorts.get(0)));
				int rPort = getRightPort(myPort);
				tempPorts.add(rPort);

				for(Integer targetPort : tempPorts){
					String msgRec="";
					Socket clientSocket = null;
					try {
						clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), targetPort);
						PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
						if(rPort == targetPort){
							pw.println("queryTrim:"+Integer.toString(myPort));
						}else{
							pw.println("queryTrim:"+Integer.toString(targetPort));
						}

						pw.flush();
						BufferedReader dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
						String value = dis.readLine();
//							clientSocket.close();

						if(value!="empty"){
							msgRec = msgRec + value;
						}


					} catch (Exception e) {
						Log.i("Excpetion QueryAll", e.getMessage());
						e.printStackTrace();
					}

					if(msgRec!=""){
						ContentValues contentValues = new ContentValues();
						if(rPort == targetPort) {
							contentValues.put("fetchAll", Integer.toString(myPort));
						}else {
							contentValues.put("fetchAll", Integer.toString(targetPort));
						}
						contentValues.put("value",msgRec);
						insert(getUri(),contentValues);
					}

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
//					clientSocket.setSoTimeout(100);
					pw.println("insert:"+key+":"+value);
					String res = dis.readLine();

					clientSocket.close();
//					Log.i("dis","dis:"+key+":targetport:"+targetPort+":"+res);
					int pport=0;
//					if(res == null){
					pport = Integer.parseInt(targetPort);

					ArrayList<Integer> targetPorts = new ArrayList<Integer>();
					targetPorts.add(getRightPort(pport));
					targetPorts.add(getRightPort(targetPorts.get(0)));

					for(int repPort:targetPorts){
						try {
							Log.i("trying replication","dis:"+key+":repPort:"+repPort);
							clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), repPort);
							pw = new PrintWriter(clientSocket.getOutputStream(), true);
							dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
							pw.println("replication:"+key+":"+value);
							res = dis.readLine();
							clientSocket.close();
							Log.i("trying status","dis:"+key+":repPort:"+repPort);
						} catch (Exception e) {
							Log.i("replication error",e.getMessage());
						}
					}

//					}
				} catch (Exception e) {
					Log.i("clientException","socket timeout "+targetPort);

				}
			}
			else  if(msgs[0].contains("replication")){
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

					} catch (Exception e) {
						Log.i("replication error",e.getMessage());
					}
				}
			}
			else  if(msgs[0].contains("single")){
				String key = msgs[1];
				try{

					int targetPort = Integer.parseInt(msgs[2]);
					Log.i("single",key+" "+targetPort);
					Socket clientSocket = null;
					String value="initial";

					try {
						Date max  = minDate;
						for(int i=0;i<3;i++){
							try{
								Date now;
//								if(targetPort==myPort){continue;}
								Log.i("finalcheck",key);
								clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), targetPort);
								PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
								BufferedReader dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
								pw.println("single:"+key);
//							clientSocket.setSoTimeout(200);
								String res = dis.readLine();

								Log.i("finalcheck","res "+res);
								clientSocket.close();
								if(res!=null && !res.contains("empty") && !res.contains("null")){

									Log.i("finalcheck","sourceport "+Integer.toString(targetPort)+":key "+key+":value "+value);
									now = formatter.parse(res.split(",")[1]);
									if(now.after(max)){
										value = res.split(",")[0];
										max = now;
									}

									Log.i("finalcheck",key+":break:"+"value"+value+":"+Integer.toString(targetPort));

//								break;
								}
//							try {
//								Thread.sleep(250);
//							}catch (Exception ee){}

							}catch (Exception exc){
								Log.i("exc","Exception +"+exc.getMessage());
							}
							targetPort = getRightPort(targetPort);
						}
						return value;

					}catch (Exception e){

						Log.i("Exception Client",e.getMessage());

					}

				}catch (Exception singleEx){
					Log.i("singleEx",key +":"+singleEx.getMessage());
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
						if(value != null){
							msgRec = msgRec + value;
						}

					} catch (Exception e) {
						Log.i("Excpetion QueryAll", e.getMessage());
						e.printStackTrace();
					}

				}
				return msgRec;
			}
			else if(msgs[0].contains("deleteOne")){

				Socket clientSocket = null;
				try {
					String key = msgs[1];
					String targetPort = msgs[2];

					clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(targetPort));
					PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
					BufferedReader dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//					clientSocket.setSoTimeout(100);
					pw.println("deleteOne:"+key);
					String res = dis.readLine();

					clientSocket.close();
//					Log.i("dis","dis:"+key+":targetport:"+targetPort+":"+res);
					int pport=0;
//					if(res == null){
					pport = Integer.parseInt(targetPort);

					ArrayList<Integer> targetPorts = new ArrayList<Integer>();
					targetPorts.add(getRightPort(pport));
					targetPorts.add(getRightPort(targetPorts.get(0)));

					for(int repPort:targetPorts){
						try {
							Log.i("trying replication","dis:"+key+":repPort:"+repPort);
							clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), repPort);
							pw = new PrintWriter(clientSocket.getOutputStream(), true);
							dis = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
							pw.println("deleteOne:"+key);
							res = dis.readLine();
							clientSocket.close();
							Log.i("trying status","dis:"+key+":repPort:"+repPort);
						} catch (Exception e) {
							Log.i("replication error",e.getMessage());
						}
					}

//					}
				} catch (Exception e) {
					Log.i("clientException","socket timeout ");

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
							contentValues.put("store","store");
//							socket.close();
							ds.println("done");
							insert(getUri(),contentValues);


						}else if(msg.contains("replication")){

							String key = msg.split(":")[1];
							String value = msg.split(":")[2];
							ds.println("done");
							ContentValues contentValues = new ContentValues();
							contentValues.put("key",key);
							contentValues.put("value",value);
							contentValues.put("replication",Integer.toString(findMatch(key)));

//							socket.close();
							insert(getUri(),contentValues);
//							socket.close();
						}
						else if(msg.contains("single")){
							String key = msg.split(":")[1];
//							Log.i("single server",key);
							MatrixCursor cursor = (MatrixCursor) fetchFile(key);
							if(cursor==null){
								ds.println("empty");
							}else{
								cursor.moveToFirst();
								String key1 = cursor.getString(cursor.getColumnIndex("key"));
								String value = cursor.getString(cursor.getColumnIndex("value"));
								ds.println(value);
							}



						}
						else if(msg.contains("queryTrim")){

							int targetPort = Integer.parseInt(msg.split(":")[1]);
							ArrayList<Integer> targetPorts = new ArrayList<Integer>();
							targetPorts.add(targetPort);

							MatrixCursor cursor = (MatrixCursor) fetchLocal(true,targetPorts);
							if(cursor==null){
								ds.println("empty");
							}else {


								String msgToSend = "";


								cursor.moveToFirst();
								do {
									String key = cursor.getString(cursor.getColumnIndex("key"));
									String value = cursor.getString(cursor.getColumnIndex("value"));

//									if(continueOrNot(key,t0,p0) || continueOrNot(key,t1,p1) || continueOrNot(key,t2,p2)){
										msgToSend = msgToSend + ":" + key + ":" + value;
//									}
								} while (cursor.moveToNext());

								if(msgToSend == ""){
									ds.println("empty");
								}else{
									ds.println(msgToSend);
								}

							}

						}
						else if(msg.contains("queryAll")){
							MatrixCursor cursor = (MatrixCursor) fetchLocal(false,REMOTE_PORTS);
							String msgToSend = "";

							Log.i("length count",""+Integer.toString(cursor.getCount()));

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
							ds.println("done");
							delete(getUri(),key+":direct",null);

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
