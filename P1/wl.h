#ifndef P1_WL_H_
#define P1_WL_H_
//
// File: wl.h
//
//  Description: This file contains the data structure for word locator.
//  Student Name: Karan Talreja
//  UW Campus ID: 9075678186
//  enamil: talreja2@wisc.edu
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <string>

/// @brief Type to specify the type of input command.
typedef enum _command {
  LOAD_e,     ///< "load" command
  LOCATE_e,   ///< "locate" command
  NEW_e,      ///< "new" command
  END_e,      ///< "end" command
  ERROR_e     ///< Default error type
} command_e;

std::string delim;
char output[1000] = {0};

/// @brief Helper macro to print to stdout using write system call.
#define wl_printf(format, ...) \
  sprintf(output, format, ##__VA_ARGS__); \
  if (write(STDOUT_FILENO, output, strlen(output)) == -1) \
    perror("Error in writing to STDOUT\n");

/**
 * @brief Function to create a deliminator string to be used for tokenizing
          input file via strtok_r.
 * @return Nothing
 */
void createDelim() {
  for (int i = 1; i < 256; i++) {
    if (i >= 48 && i <= 57) continue;   // [0-9] cannot end word
    if (i >= 65 && i <= 90) continue;   // [A-Z] cannot end word
    if (i >= 97 && i <= 122) continue;  // [a-z] cannot end word
    if (i == 39) continue;              // '(Apostrophe) cannot end word
    delim += (char)i;
  }
}

/**
 * @brief Function to check whether the input command argument contains
          correct characters.
 * @retval true   If no character is out of the range of valid characters
 * @retval false  If any character is out of the range of valid characters
 */
bool checkCorrectChars(const char* word) {
  for (; *word != '\0'; word++) {
    int i = *word;
    if (i >= 48 && i <= 57) continue;         // [0-9] valid
    else if (i >= 65 && i <= 90) continue;    // [A-Z] valid
    else if (i >= 97 && i <= 122) continue;   // [a-z] valid
    else if (i == 39) continue;               // '(Apostrophe) cannot end word
    else if (i == 0) continue;
    else
      return false;
  }
  return true;
}

/// @brief Class to implement vector type functionality.
class myVector {
 public:
    int* arr;         ///< Member variable to store indices in array
    size_t capacity;  ///< Member variable to store alloced memory to data
    size_t m_size;    ///< Member variable to store actual data in structure

    /// @brief Default constructor which sets all member pointers to NULL
    myVector():arr(NULL), capacity(0), m_size(0) {}

    /// @brief Member function to return size of the data structure
    size_t size() {
      return m_size;
    }

    /**
     * @brief Member function to add an integer at the back of the data
     structure
     * @param idx Input index which is to be added.
     * @return Nothing
     */
    void push_back(int idx) {
      if (m_size + 1 > capacity) {
        if (capacity == 0) {
          capacity = 2;
          arr = (int*)malloc(capacity*sizeof(int));
          arr[m_size++] = idx;
        } else {
          capacity *= 2;
          arr = (int*)realloc(arr, capacity*sizeof(int));
          arr[m_size++] = idx;
        }
      } else {
        arr[m_size++] = idx;
      }
    }

    /**
     * @brief Operator overload to provide array like acess to calling routine
     * @param index Index of the data which is to be accessed.
     * @return Data at the input index.
     */
    int operator[] (int index) {
      return arr[index];
    }

    /// @brief Destructor for memory deallocation.
    ~myVector() {
      if (NULL != arr) free(arr);
    }
};

/**
 * @brief Class to represent the node of a binary search tree.
 */
class node {
 public:
    node* left;       ///< Member Pointer to left subtree of this node.
    node* right;      ///< Member Pointer to right subtree of this node.
    char* word;       ///< Member for Word stored in this node.
    myVector* index;  ///< Indices of this word in the input document

    /// @brief Default constructor which sets all member pointers to NULL
    node():left(NULL), right(NULL), word(NULL), index(NULL) {}

    /**
     * @brief One argument constructor with input argument to set word.
     * @param word  Input word to be stored in this node.
     */
    node(char* word):left(NULL), right(NULL), word(word), index(new \
    myVector()) {}

    /// @brief Destructor for memory deallocation.
    ~node() {
      if (NULL != word) free(word);
      if (NULL != left) delete left;
      if (NULL != right) delete right;
      if (NULL != index)  delete index;
    }
} *root;

/**
 * @brief Function to insert a word into binary search tree.
 * @param	word	word from the document which is to be inserted into the tree.
 * @param index Index at which this word was found in the document.
 * @return The new root of the binary search tree.
 */
node* insert(node* root, const char* word, int index) {
  if (root == NULL) {
    root = new node(strdup(word));
    root->index->push_back(index);
    return root;
  }
  int rc = strcasecmp(word, root->word);
  if (rc < 0) {
    node* retNode = insert(root->left, word, index);
    if (root->left == NULL) root->left = retNode;
  } else if (rc > 0) {
    node* retNode = insert(root->right, word, index);
    if (root->right == NULL) root->right = retNode;
  } else {
    root->index->push_back(index);
  }
  return root;
}

/**
 * @brief Function to lookup a word from the binary search tree.
 * @param	root	The root of the binary search tree in which the node is to be 
 * looked up.
 * @param word  The word which is to be looked for
 * @details The function returns if the input is NULL. If not it checks if the 
 * input word is equal to, less than or greater than the current node's word. 
 * If it's equal current node is returned. Otherwise the word is lookedup into
 * left subtree or right sub tree.
 * @return The node which contains the information for the query word. If it 
 * doesn't exist NULL is returned.
 */
node* lookup(node* root, const char* word) {
  if (root == NULL) return NULL;
  node* retNode = NULL;
  int rc = strcasecmp(word, root->word);
  if (rc < 0) retNode = lookup(root->left, word);
  else if (rc > 0) retNode = lookup(root->right, word);
  else
    retNode = root;
  return retNode;
}

/**
 * @brief Function to do a inorder traversal on the binary search tree.
 * @param	root	The root of the binary search tree on which in order traversal 
 is to be done.
 * @return Nothing.
 */
void inorder(node* root) {
  if (root == NULL) return;
  inorder(root->left);
  wl_printf("%s ", root->word);
  for (unsigned int i = 0; i < root->index->m_size; i++) {
    wl_printf("%d ", (*root->index)[i]);
  }
  wl_printf("\n");
  inorder(root->right);
}
#endif  // P1_WL_H_
