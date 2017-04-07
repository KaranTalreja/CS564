#include <stdio.h>
#include "load.h"

using namespace std;

int main(int argc, char* argv[])
{
  sqlite3 *db;
  int conn;

  conn = sqlite3_open("nutrients.db", &db); //sqlite3 api

  if( conn ){
    fprintf(stderr, "Unable to open the database: %s\n", sqlite3_errmsg(db)); //sqlite3 api
    return(0);
  }else{
    fprintf(stderr, "database opened successfully\n");
  }
  init_data();
  loadTables(db);

  sqlite3_close(db); //sqlite3 api
}
