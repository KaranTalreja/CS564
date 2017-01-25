//
// File: wl.cpp
//
//  Description: This file contains the implementation of word locator.
//  Student Name: Karan Talreja
//  UW Campus ID: 9075678186
//  enamil: talreja2@wisc.edu

#include "wl.h"

/**
 * @brief main function
 * @param argc Input number of arguments
 * @param argv Input arguments
 */
int main(int argc, char** argv) {
  char command[514];
  char* currCommand = command, *token = command;
  FILE* fin = stdin, *currLoadFile = NULL;
  char* rc = NULL;
  command_e cmd = ERROR_e;
  createDelim();
  while (1) {
    cmd = ERROR_e;
    wl_printf(">");
    rc = fgets(command, sizeof(command), fin);
    if (rc == NULL) continue;

    char* context;
    char* argvs[4];
    int i = 0;
    currCommand = command;
    rc = strchr(currCommand, '\n');
    if (NULL != rc) *rc = '\0';

    while (NULL != (token = strtok_r(currCommand, " ", &context))) {
      currCommand = NULL;
      argvs[i++] = token;
      if (i > 3) break;
    }

    if (i == 1) {
      if (0 == strcasecmp(argvs[0], "new")) cmd = NEW_e;
      else if (0 == strcasecmp(argvs[0], "end")) cmd = END_e;
    } else if (i == 2) {
      if (0 == strcasecmp(argvs[0], "load")) cmd = LOAD_e;
    } else if (i == 3) {
      if (0 == strcasecmp(argvs[0], "locate")) cmd = LOCATE_e;
    } else {
        wl_printf("ERROR: Invalid command\n");
        continue;
    }

    switch (cmd) {
      case LOAD_e: {
        currLoadFile = fopen(argvs[1], "r");
        if (NULL == currLoadFile) {
          wl_printf("ERROR: Invalid command\n");
          continue;
        }
        if (NULL != root) {
          delete root;
          root = NULL;
        }
        char line[100];
        int idx = 1;
        while (NULL != fgets(line, sizeof(line), currLoadFile)) {
          currCommand = line;
          while (NULL != (token = strtok_r(currCommand, delim.c_str(), \
            &context))) {
            currCommand = NULL;
            std::string temp(token);
            root = insert(root, temp.c_str(), idx);
            idx++;
          }
        }
        fclose(currLoadFile);
        // inorder(root);
      }
        break;
      case LOCATE_e: {
        char* err = NULL;
        int occurence = strtol(argvs[2], &err, 10);
        if (*err != '\0') {
          wl_printf("ERROR: Invalid command\n");
          continue;
        }
        if (occurence < 0) {
          wl_printf("ERROR: Invalid command\n");
          continue;
        }
        if (false == checkCorrectChars(argvs[1])) {
          wl_printf("ERROR: Invalid command\n");
          continue;
        }
        std::string temp(argvs[1]);
        node* rc = lookup(root, temp.c_str());
        if (rc == NULL) {
          wl_printf("No matching entry\n");
        } else if (rc->index->size() < (unsigned int)occurence) {
          wl_printf("No matching entry\n");
        } else {
          wl_printf("%d\n", (*(rc->index))[occurence - 1]);
        }
      }
        break;
      case NEW_e:
        if (NULL != root) {
          delete root;
          root = NULL;
        }
        break;
      case END_e:
        if (NULL != root) {
          delete root;
          root = NULL;
        }
        return 0;
        break;
      default:
        wl_printf("ERROR: Invalid command\n");
        continue;
    }
  }
  return 0;
}
