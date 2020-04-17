package com.example.learn4;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.os.Bundle;
import android.service.autofill.TextValueSanitizer;
import android.util.Log;
import android.view.ActionMode;
import android.view.Menu;
import android.view.MenuInflater;
import android.view.MenuItem;
import android.view.View;
import android.widget.AbsListView;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.ListView;
import android.widget.TextView;

import java.util.ArrayList;

public class MainActivity extends AppCompatActivity {

    static ArrayList<String> noteList;
    static ArrayAdapter<String> noteListAdapter;

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        MenuInflater menuInflater =  getMenuInflater();
        menuInflater.inflate(R.menu.add_note,menu);
        return super.onCreateOptionsMenu(menu);
    }

    @Override
    public boolean onOptionsItemSelected(@NonNull MenuItem item) {
        Log.i("note Activity","called");
        super.onContextItemSelected(item);
        if(item.getItemId() == R.id.addNote){
            Log.i("note Activity","called");
            Intent intent = new Intent(getApplicationContext(),NoteEditorActivity.class);
            startActivity(intent);
            return true;
        }

        return false;
    }



    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);


        SQLiteDatabase mySql = this.openOrCreateDatabase("Accounts",MODE_PRIVATE,null);
        mySql.execSQL("CREATE TABLE IF NOT EXISTS users(name VARCHAR, age INT(3))");
        mySql.execSQL("INSERT INTO users VALUES('vamsi',24)");

        Cursor cursor = mySql.rawQuery("SELECT*FROM users",null);

        int name = cursor.getColumnIndex("name");
        int age = cursor.getColumnIndex("age");
        cursor.moveToFirst();

        while (!cursor.isAfterLast()){
            Log.i("cursor name",cursor.getString(name));
            Log.i("cursor age",cursor.getString(age));
            cursor.moveToNext();
        }







        noteList = new ArrayList<String>();
        noteList.add("Hey Bro");noteList.add("Working");noteList.add("Hey Whats");
        noteListAdapter = new ArrayAdapter<String>(this,android.R.layout.simple_list_item_1,noteList);

        ListView noteListView = (ListView) findViewById(R.id.noteList);
        noteListView.setAdapter(noteListAdapter);

        noteListView.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> adapterView, View view, int i, long l) {
                TextView tx = (TextView) view;
                Log.i("view",tx.getText().toString());

                Intent intent = new Intent(getApplicationContext(), NoteEditorActivity.class);
                intent.putExtra("noteId",i);
                startActivity(intent);
            }
        });




    }
}
