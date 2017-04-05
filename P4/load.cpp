#include <stdio.h>
#include "sqlite3.h"

int main(int argc, char* argv[])
{
  sqlite3 *db;
  int conn;

  conn = sqlite3_open("sample.db", &db); //sqlite3 api

  if( conn ){
    fprintf(stderr, "Unable to open the database: %s\n", sqlite3_errmsg(db)); //sqlite3 api
    return(0);
  }else{
    fprintf(stderr, "database opened successfully\n");
  }
  sqlite3_close(db); //sqlite3 api
}
