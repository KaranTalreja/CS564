#ifndef _LOAD_H
#define _LOAD_H

#include <string>
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <vector>
#include <algorithm>
#include "sqlite3.h"
#include "assert.h"

using namespace std;

#define DEBUG
//#define SHOW_COMMAND

vector<const char*> TABLES = {"FOOD_DES", "NUT_DATA", "WEIGHT", "FOOTNOTE"};

vector<const char*> TABLE_FILE = {"data/FOOD_DES.txt", "data/NUT_DATA.txt", "data/WEIGHT.txt", "data/FOOTNODE.txt"};

vector<vector<const char*> > TABLE_DTYPE = {
    {"CHAR(5) PRIMARY KEY NOT NULL", "CHAR(4) NOT NULL REFERENCES FD_GROUP(FdGrp_Cd)", "CHAR(200) NOT NULL", "CHAR(60) NOT NULL", "CHAR(100)", "CHAR(65)", "CHAR", "CHAR(135)", "INT", "CHAR(65)", "REAL", "REAL", "REAL", "REAL"},
    {}
};

vector<vector<const char*> > TABLE_ATTR = {
{"NDB_No" ,"FdGrp_Cd" ,"Long_Desc" ,"Shrt_Desc" ,"ComName" ,"ManufacName" ,"Survey" ,"Ref_desc" ,"Refuse" ,"SciName" ,"N_Factor" ,"Pro_Factor" ,"Fat_Factor" ,"CHO_Factor"},
{},
{},
{}
};

void createTable (sqlite3* db ,int idx) {
  string sql = "CREATE TABLE " + string(TABLES[idx]) +" (";
  int size = TABLE_DTYPE[idx].size();
  for (int i = 0; i < size; i++) {
    sql += string(TABLE_ATTR[idx][i]) + string(" ") + string(TABLE_DTYPE[idx][i]);
    if (i < size-1) sql += string(",");
  }
  sql += string(");");
  int rc = sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL);
#ifdef DEBUG
  assert(rc == 0);
#endif
#ifdef SHOW_COMMAND
  cout << sql << endl;
#endif
}

void loadTables(sqlite3* db) {
  
  int numberOfTables = 1;//TABLES.size();
  for (int t = 0; t < numberOfTables; t++) {
    createTable(db, t);
    FILE* currLoadFile = fopen(TABLE_FILE[t], "r");
    char line[2048], *context, *token, *currCommand;
    int totalAttr = TABLE_ATTR[t].size();
    while (NULL != fgets(line, sizeof(line), currLoadFile)) {
      string command = "INSERT INTO " + string(TABLES[t]) + string(" VALUES (");
      currCommand = line;
      for (int i = 0; i < totalAttr; i++) {
        if (NULL != (token = strtok_r(currCommand, "^\r\n", &context))) {
          currCommand = NULL;
          string temp(token);
          replace(temp.begin(), temp.end(), '"', '\'');
          command += temp;
          if (i < totalAttr - 1) command += string(",");
        } else {
          command += string("\"\"");
          if (i < totalAttr - 1) command += string(",");
        }
      }
      command += string(");");
      replace(command.begin(), command.end(), '~', '"');
      char* errMsg;
#ifdef SHOW_COMMAND
      cout << command << endl;
#endif
      int rc = sqlite3_exec(db, command.c_str(), NULL, NULL, &errMsg);

#ifdef DEBUG
      if (rc != 0) {
        cout << errMsg << endl;
      }
      assert(rc == 0);
#endif
    }
    fclose(currLoadFile);
  }

}

#endif
