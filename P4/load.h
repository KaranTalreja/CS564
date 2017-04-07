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
#define SHOW_COMMAND

vector<const char*> TABLES = {"FOOD_DES", "FD_GROUP", "LANGUAL", "LANGDESC", "NUT_DATA", "NUTR_DEF", "SRC_CD",
    "DERIV_CD", "WEIGHT", "FOOTNOTE", "DATSRCLN", "DATA_SRC"};

vector<const char*> TABLE_FILE = {"data/FOOD_DES.txt", "data/FD_GROUP.txt", "data/LANGUAL.txt", "data/LANGDESC.txt",
    "data/NUT_DATA.txt", "data/NUTR_DEF.txt", "data/SRC_CD.txt", "data/DERIV_CD.txt", "data/WEIGHT.txt", "data/FOOTNOTE.txt", "data/DATSRCLN.txt",
    "data/DATA_SRC.txt"};

vector<vector<const char*> > TABLE_DTYPE = {
    {"CHAR(5) PRIMARY KEY NOT NULL",
     "CHAR(4) NOT NULL REFERENCES FD_GROUP(FdGrp_Cd)",
     "CHAR(200) NOT NULL",
     "CHAR(60) NOT NULL",
     "CHAR(100)",
     "CHAR(65)",
     "CHAR",
     "CHAR(135)",
     "INT",
     "CHAR(65)",
     "REAL",
     "REAL",
     "REAL",
     "REAL"},

    {"CHAR(4) PRIMARY KEY NOT NULL",
     "CHAR(60) NOT NULL"},

    {"CHAR(5) NOT NULL REFERENCES FOOD_DES(NDB_No)",
     "CHAR(5) NOT NULL REFERENCES LANGDESC(Factor_Code) , PRIMARY KEY (NDB_No, Factor_Code)"},

    {"CHAR(5) PRIMARY KEY NOT NULL",
     "CHAR(140) NOT NULL"},

    {"CHAR(5) NOT NULL REFERENCES FOOD_DES(NDB_No)",
     "CHAR(3) NOT NULL REFERENCES NUTR_DEF(Nutr_No)",
     "REAL NOT NULL",
     "REAL NOT NULL",
     "REAL",
     "CHAR(2) NOT NULL REFERENCES SRC_CD(Src_Cd)",
     "CHAR(4) REFERENCES DERIV_CD(Deriv_Cd)",
     "CHAR(5) REFERENCES FOOD_DES(NDB_No)",
     "CHAR(12)",
     "REAL",
     "REAL",
     "REAL",
     "REAL",
     "REAL",
     "REAL",
     "CHAR(10)",
     "CHAR(10)",
     "CHAR(1) , PRIMARY KEY (NDB_No, Nutr_No)"},

     {"CHAR(3) NOT NULL REFERENCES NUT_DATA(Nutr_No)",
      "CHAR(7) NOT NULL",
      "CHAR(20)",
      "CHAR(60) NOT NULL",
      "CHAR(1) NOT NULL",
      "INT NOT NULL, PRIMARY KEY (Nutr_No)"},

     {"CHAR(2) PRIMARY KEY NOT NULL",
      "CHAR(60) NOT NULL"},

     {"CHAR(4) PRIMARY KEY NOT NULL",
      "CHAR(120) NOT NULL"},

     {"CHAR(5) NOT NULL REFERENCES FOOD_DES(NDB_No)",
      "CHAR(2) NOT NULL",
      "REAL NOT NULL",
      "CHAR(84) NOT NULL",
      "REAL NOT NULL",
      "INT",
      "REAL, PRIMARY KEY (NDB_No, Seq)"}
    ,

         {"CHAR(5) NOT NULL REFERENCES FOOD_DES(NDB_No)",
          "CHAR(4) NOT NULL",
          "CHAR(1) NOT NULL",
          "CHAR(3)",
          "CHAR(200) NOT NULL"},

     {"CHAR(5) NOT NULL REFERENCES NUT_DATA(NDB_No)",
      "CHAR(3) NOT NULL REFERENCES NUTR_DEF(Nutr_No)",
      "CHAR(6) NOT NULL REFERENCES DATA_SRC(DataSrc_ID), PRIMARY KEY (NDB_No, Nutr_No, DataSrc_ID)"},

     {"CHAR(6) PRIMARY KEY NOT NULL",
      "CHAR(255)",
      "CHAR(255) NOT NULL",
      "CHAR(4)",
      "CHAR(135)",
      "CHAR(16)",
      "CHAR(5)",
      "CHAR(5)",
      "CHAR(5)",}
};

vector<vector<const char*> > TABLE_ATTR = {
{"NDB_No" ,
 "FdGrp_Cd" ,
 "Long_Desc" ,
 "Shrt_Desc" ,
 "ComName" ,
 "ManufacName" ,
 "Survey" ,
 "Ref_desc" ,
 "Refuse" ,
 "SciName" ,
 "N_Factor" ,
 "Pro_Factor" ,
 "Fat_Factor" ,
 "CHO_Factor"},

{"FdGrp_Cd",
 "FdGrp_Desc"},

{"NDB_No",
 "Factor_Code"},

{"Factor_Code",
 "Description"},

{"NDB_No",
 "Nutr_No",
 "Nutr_Val",
 "Num_Data_Pts",
 "Std_Error",
 "Src_Cd",
 "Deriv_Cd",
 "Ref_NDB_No",
 "Add_Nutr_Mark",
 "Num_Studies",
 "Min",
 "Max",
 "DF",
 "Low_EB",
 "Up_EB",
 "Stat_cmt",
 "AddMod_Date",
 "CC"},

 {"Nutr_No",
  "Units",
  "Tagname",
  "NutrDesc",
  "Num_Dec",
  "SR_Order"},

  {"Src_Cd",
   "SrcCd_Desc"},

  {"Deriv_Cd",
   "Deriv_Desc"},

   {"NDB_No",
    "Seq",
    "Amount",
    "Msre_Desc",
    "Gm_Wgt",
    "Num_Data_Pts",
    "Std_Dev"},

    {"NDB_No",
     "Footnt_No",
     "Footnt_Typ",
     "Nutr_No",
     "Footnt_Txt"},

     {"NDB_No",
      "Nutr_No",
      "DataSrc_ID"},

     {"DataSrc_ID",
      "Authors",
      "Title",
      "Year",
      "Journal",
      "Vol_City",
      "Issue_State",
      "Start_Page",
      "End_Page"}
};

void createTable (sqlite3* db ,int idx) {
  string dropTable = "DROP TABLE IF EXISTS " + string(TABLES[idx])  + ";";
  char* errMsg;
  int rc = sqlite3_exec(db, dropTable.c_str(), NULL, NULL, NULL);
#ifdef DEBUG
  if (rc != 0) {
    cout << errMsg << endl;
  }
  assert(rc == 0);
#endif
  string sql = "CREATE TABLE " + string(TABLES[idx]) +" (";
  int size = TABLE_DTYPE[idx].size();
  for (int i = 0; i < size; i++) {
    sql += string(TABLE_ATTR[idx][i]) + string(" ") + string(TABLE_DTYPE[idx][i]);
    if (i < size-1) sql += string(",");
  }
  sql += string(");");

  rc = sqlite3_exec(db, sql.c_str(), NULL, NULL, &errMsg);
#ifdef DEBUG
  if (rc != 0) {
    cout << errMsg << endl;
  }
  assert(rc == 0);
#endif
#ifdef SHOW_COMMAND
  cout << sql << endl;
#endif
}

void replaceAll(std::string& str, const std::string& from, const std::string& to) {
    if(from.empty())
        return;
    size_t start_pos = 0;
    while((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); // In case 'to' contains 'from', like replacing 'x' with 'yx'
    }
}

void loadTables(sqlite3* db) {
  
  int numberOfTables = TABLES.size();
  for (int t = 0; t < numberOfTables; t++) {
    createTable(db, t);
    FILE* currLoadFile = fopen(TABLE_FILE[t], "r");
    char line[2048], *context, *token, *currCommand;
    int totalAttr = TABLE_ATTR[t].size();
    sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    while (NULL != fgets(line, sizeof(line), currLoadFile)) {
      string command = "INSERT INTO " + string(TABLES[t]) + string(" VALUES (");
      currCommand = line;
      for (int i = 0; i < totalAttr; i++) {
        if (NULL != (token = strtok_r(currCommand, "^\r\n", &context))) {
          currCommand = NULL;
          string temp(token);
          replaceAll(temp, "'", "''");
          command += temp;
          if (i < totalAttr - 1) command += string(",");
        } else {
          command += string("\"\"");
          if (i < totalAttr - 1) command += string(",");
        }
      }
      command += string(");");
      replace(command.begin(), command.end(), '~', '\'');
      char* errMsg;
#ifdef SHOW_COMMAND
      //cout << command << endl;
#endif
      int rc = sqlite3_exec(db, command.c_str(), NULL, NULL, &errMsg);

#ifdef DEBUG
      if (rc != 0) {
        cout << errMsg << endl;
      }
      assert(rc == 0);
#endif
    }
    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    fclose(currLoadFile);
  }

}

#endif
