package com.example.learn4;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.text.Editable;
import android.text.TextWatcher;
import android.widget.EditText;

public class NoteEditorActivity extends AppCompatActivity {
    int noteId;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_note_editor);

        EditText editText = (EditText) findViewById(R.id.noteEditor);
        Intent intent = getIntent();
        noteId =  intent.getIntExtra("noteId",-1);
        if(noteId == -1){
            MainActivity.noteList.add("Hey Welcome");
            MainActivity.noteListAdapter.notifyDataSetChanged();
//            return;
            noteId = MainActivity.noteList.size() - 1;
        }
        editText.setText(MainActivity.noteList.get(noteId));

        editText.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence charSequence, int i, int i1, int i2) {

            }

            @Override
            public void onTextChanged(CharSequence charSequence, int i, int i1, int i2) {

                MainActivity.noteList.set(noteId,charSequence.toString());
                MainActivity.noteListAdapter.notifyDataSetChanged();

            }

            @Override
            public void afterTextChanged(Editable editable) {

            }
        });
    }
}
