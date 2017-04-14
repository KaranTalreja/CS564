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
#include "stdarg.h"

using namespace std;

//#define DEBUG
//#define SHOW_COMMAND

/*Function to initialize a vector of strings*/
void initializeVec (vector<const char*>& in, int num, const char* n_char, ...)
{
    va_list valist;
    int i;

    /* initialize valist for num number of arguments */
    va_start(valist, n_char);

    in.push_back(n_char);
    /* access all the arguments assigned to valist */
    for (i = 0; i < num-1; i++) {
      const char* str = va_arg(valist, const char*);
      in.push_back(str);
    }

    /* clean memory reserved for valist */
    va_end(valist);
}

vector<const char*> TABLES;

vector<const char*> TABLE_FILE;

vector<vector<const char*> > TABLE_DTYPE;
vector<const char*> TABLE_DTYPE1;
vector<const char*> TABLE_DTYPE2;
vector<const char*> TABLE_DTYPE3;
vector<const char*> TABLE_DTYPE4;
vector<const char*> TABLE_DTYPE5;
vector<const char*> TABLE_DTYPE6;
vector<const char*> TABLE_DTYPE7;
vector<const char*> TABLE_DTYPE8;
vector<const char*> TABLE_DTYPE9;
vector<const char*> TABLE_DTYPE10;
vector<const char*> TABLE_DTYPE11;
vector<const char*> TABLE_DTYPE12;


vector<const char*> TABLE_ATTR1;
vector<const char*> TABLE_ATTR2;
vector<const char*> TABLE_ATTR3;
vector<const char*> TABLE_ATTR4;
vector<const char*> TABLE_ATTR5;
vector<const char*> TABLE_ATTR6;
vector<const char*> TABLE_ATTR7;
vector<const char*> TABLE_ATTR8;
vector<const char*> TABLE_ATTR9;
vector<const char*> TABLE_ATTR10;
vector<const char*> TABLE_ATTR11;
vector<const char*> TABLE_ATTR12;

vector<vector<const char*> > TABLE_ATTR;

/* Function to initialize data in vectors to be used by create and load table routines*/
void init_data() {
  initializeVec(TABLES, 12,"FOOD_DES", "FD_GROUP", "LANGUAL", "LANGDESC", "NUT_DATA", "NUTR_DEF", "SRC_CD",
      "DERIV_CD", "WEIGHT", "FOOTNOTE", "DATSRCLN", "DATA_SRC");
  initializeVec(TABLE_FILE,12, "FOOD_DES.txt", "FD_GROUP.txt", "LANGUAL.txt", "LANGDESC.txt",
      "NUT_DATA.txt", "NUTR_DEF.txt", "SRC_CD.txt", "DERIV_CD.txt", "WEIGHT.txt", "FOOTNOTE.txt", "DATSRCLN.txt",
      "DATA_SRC.txt");
  initializeVec(TABLE_DTYPE1,14, "CHAR(5) PRIMARY KEY NOT NULL",
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
     "REAL");
 
  initializeVec(TABLE_DTYPE2,2,"CHAR(4) PRIMARY KEY NOT NULL",
     "CHAR(60) NOT NULL");

  initializeVec(TABLE_DTYPE3,2,"CHAR(5) NOT NULL REFERENCES FOOD_DES(NDB_No)",
     "CHAR(5) NOT NULL REFERENCES LANGDESC(Factor_Code) , PRIMARY KEY (NDB_No, Factor_Code)");
  initializeVec(TABLE_DTYPE4,2,"CHAR(5) PRIMARY KEY NOT NULL",
     "CHAR(140) NOT NULL");
  initializeVec(TABLE_DTYPE5,18,"CHAR(5) NOT NULL REFERENCES FOOD_DES(NDB_No)",
     "CHAR(3) NOT NULL REFERENCES NUTR_DEF(Nutr_No)",
     "NUMERIC(10,3) NOT NULL",
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
     "CHAR(1) , PRIMARY KEY (NDB_No, Nutr_No)");
  initializeVec(TABLE_DTYPE6,6,"CHAR(3) NOT NULL REFERENCES NUT_DATA(Nutr_No)",
      "CHAR(7) NOT NULL",
      "CHAR(20)",
      "CHAR(60) NOT NULL",
      "CHAR(1) NOT NULL",
      "INT NOT NULL, PRIMARY KEY (Nutr_No)");
  initializeVec(TABLE_DTYPE7,2,"CHAR(2) PRIMARY KEY NOT NULL",
      "CHAR(60) NOT NULL");
  initializeVec(TABLE_DTYPE8,2,"CHAR(4) PRIMARY KEY NOT NULL",
      "CHAR(120) NOT NULL");
  initializeVec(TABLE_DTYPE9,7,"CHAR(5) NOT NULL REFERENCES FOOD_DES(NDB_No)",
      "CHAR(2) NOT NULL",
      "REAL NOT NULL",
      "CHAR(84) NOT NULL",
      "REAL NOT NULL",
      "INT",
      "REAL, PRIMARY KEY (NDB_No, Seq)");
  initializeVec(TABLE_DTYPE10,5,"CHAR(5) NOT NULL REFERENCES FOOD_DES(NDB_No)",
          "CHAR(4) NOT NULL",
          "CHAR(1) NOT NULL",
          "CHAR(3)",
          "CHAR(200) NOT NULL");
  initializeVec(TABLE_DTYPE11,3,"CHAR(5) NOT NULL REFERENCES NUT_DATA(NDB_No)",
      "CHAR(3) NOT NULL REFERENCES NUTR_DEF(Nutr_No)",
      "CHAR(6) NOT NULL REFERENCES DATA_SRC(DataSrc_ID), PRIMARY KEY (NDB_No, Nutr_No, DataSrc_ID)");
  initializeVec(TABLE_DTYPE12,9,"CHAR(6) PRIMARY KEY NOT NULL",
      "CHAR(255)",
      "CHAR(255) NOT NULL",
      "CHAR(4)",
      "CHAR(135)",
      "CHAR(16)",
      "CHAR(5)",
      "CHAR(5)",
      "CHAR(5)");

  TABLE_DTYPE.push_back(TABLE_DTYPE1);
  TABLE_DTYPE.push_back(TABLE_DTYPE2);
  TABLE_DTYPE.push_back(TABLE_DTYPE3);
  TABLE_DTYPE.push_back(TABLE_DTYPE4);
  TABLE_DTYPE.push_back(TABLE_DTYPE5);
  TABLE_DTYPE.push_back(TABLE_DTYPE6);
  TABLE_DTYPE.push_back(TABLE_DTYPE7);
  TABLE_DTYPE.push_back(TABLE_DTYPE8);
  TABLE_DTYPE.push_back(TABLE_DTYPE9);
  TABLE_DTYPE.push_back(TABLE_DTYPE10);
  TABLE_DTYPE.push_back(TABLE_DTYPE11);
  TABLE_DTYPE.push_back(TABLE_DTYPE12);

  initializeVec(TABLE_ATTR1,14,"NDB_No" ,
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
 "CHO_Factor");
 
  initializeVec(TABLE_ATTR2,2,"FdGrp_Cd",
 "FdGrp_Desc");

  initializeVec(TABLE_ATTR3,2,"NDB_No",
 "Factor_Code");
  initializeVec(TABLE_ATTR4,2,"Factor_Code",
 "Description");
  initializeVec(TABLE_ATTR5,18,"NDB_No",
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
 "CC");
  initializeVec(TABLE_ATTR6,6,"Nutr_No",
  "Units",
  "Tagname",
  "NutrDesc",
  "Num_Dec",
  "SR_Order");
  initializeVec(TABLE_ATTR7,2,"Src_Cd",
   "SrcCd_Desc");
  initializeVec(TABLE_ATTR8,2,"Deriv_Cd",
   "Deriv_Desc");
  initializeVec(TABLE_ATTR9,7,"NDB_No",
    "Seq",
    "Amount",
    "Msre_Desc",
    "Gm_Wgt",
    "Num_Data_Pts",
    "Std_Dev");
  initializeVec(TABLE_ATTR10,5,"NDB_No",
     "Footnt_No",
     "Footnt_Typ",
     "Nutr_No",
     "Footnt_Txt");
  initializeVec(TABLE_ATTR11,3,"NDB_No",
      "Nutr_No",
      "DataSrc_ID");
  initializeVec(TABLE_ATTR12,9,"DataSrc_ID",
      "Authors",
      "Title",
      "Year",
      "Journal",
      "Vol_City",
      "Issue_State",
      "Start_Page",
      "End_Page");
  TABLE_ATTR.push_back(TABLE_ATTR1);
  TABLE_ATTR.push_back(TABLE_ATTR2);
  TABLE_ATTR.push_back(TABLE_ATTR3);
  TABLE_ATTR.push_back(TABLE_ATTR4);
  TABLE_ATTR.push_back(TABLE_ATTR5);
  TABLE_ATTR.push_back(TABLE_ATTR6);
  TABLE_ATTR.push_back(TABLE_ATTR7);
  TABLE_ATTR.push_back(TABLE_ATTR8);
  TABLE_ATTR.push_back(TABLE_ATTR9);
  TABLE_ATTR.push_back(TABLE_ATTR10);
  TABLE_ATTR.push_back(TABLE_ATTR11);
  TABLE_ATTR.push_back(TABLE_ATTR12);
}

/*
  Function to create tables using data initialized above
*/
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

/* Function to load all the tables using data initialized above*/
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
