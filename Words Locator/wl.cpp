/**
 * @file    wl.cpp
 * @author  rshen
 * @version 1.0
 *
 * @section DESCRIPTION
 *
 * This program is a word locator for CS564 Fall2019 homework one
 *
 * This program contains two file within the submission that are wl.cpp and
 * wl.h files. But the wl.h does not really use for the sharing between source
 * files since the only source file is wl.cpp. The main fuction of this program
 * are loading text files and store the words into the red-black tree sturcture,
 * and locating the given key words and output the position.
 */

#include <cxxabi.h>
#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <iterator>
#include <string.h>

using namespace std;

/**
 * Defining Node structure in the global space
 *
 * The Node structure contains name, the pionter to the
 * left node and the right node, and the word counter.
 */
typedef struct Node
{
    string name;
    int wordCount;
    struct Node* left;
    struct Node* right;
}Node;


/**
 * Defining the Binary Serch Tree structure in the global space
 *
 * The RBTree structure contains the root pionter of the tree node.
 */
typedef struct
{
    Node* root;
}RBtree;

// create instance of the class
RBtree* wordTree = new RBtree;
/**
 * Insert the added node into data structure
 *
 * This method is helping the loading method with adding the new node from
 * the new loaded file into the data structure. This method recursive inserting
 * and adding new nodes into the binary search tree.
 *
 * @param root the root ptr of TreeNode
 * @param addnode the new node to be added
 */
void Insert(Node *root, Node *new_node) {
    
    
    if (new_node->name < root->name) {
        if (root->left == NULL)
            root->left = new_node;
        else
            Insert(root->left, new_node);
    }
    
    else {
        if (root->right == NULL)
            root->right = new_node;
        else
            Insert(root->right, new_node);
    }
}

/**
 * Loading text files and documents, parse and store the words in
 * the BST date structure.
 *
 * This method checks if the given file is vaild when it is loading,
 * if the file is open and load succesully, the insert method will
 * do the parsing and storing.
 *
 * @param fileName the name of laoding file
 */
void load(string file){
    
    //fields
    string wordRaw;
    
    //attempt to open file
    ifstream infile(file.c_str());
    
    //load in new file nd add to array
    
    
    int count = 1;
    while (infile >> wordRaw)
    {
        //word to be added
        string word;
        Node* addWord = new Node;
        
        // all puncuation other than apostrophe will treated as white space
        remove_copy_if(wordRaw.begin(), wordRaw.end(), back_inserter(word), [](char ch){ return !(std::isalpha(ch) || isdigit(ch) || ch == '\''); });
        wordRaw.erase (wordRaw.begin(), wordRaw.end());
        transform(word.begin(), word.end(), word.begin(), ::tolower);
        
        //intialize fields
        addWord ->name = word;
        addWord->wordCount = count;
        addWord->left = NULL;
        addWord->right = NULL;
        
        //insert node
        if(wordTree->root == NULL){
            wordTree->root = addWord;
        }else {
            Insert(wordTree->root, addWord);
        }
        
        count ++;
    }
    
    
}


/**
 * Locating the given key within the data structure
 *
 * This method is the main usage of the locate function of this project.
 * It counts the occurence of the given key and figure out the position of
 * the given key. The method recursive locating the given key within the
 * binary search tree.
 *
 * @param root the root ptr of TreeNode
 * @param key  the given key from input stream
 * @param idx  ptr to the index of the given key
 * @param curr the current occurence of the key
 */
int locate(Node* root, string word, int occurence, int* pt){

    // locating fail if root is null
    if (root == NULL) {
        *pt = 1;
        return 0;
    }
    
    //base case 
    if (word == root->name) {
        
        
        if(*pt == occurence ){
            *pt = 1;
            return root->wordCount;
        }else{
            *pt = *pt +1;
            return locate(root->right, word, occurence, pt);
        }
        
    }
    
    else if (word < root->name) {
        return locate(root->left, word, occurence, pt);
    }
    
    else if (word > root->name) {
        return locate(root->right, word, occurence, pt);
    }
}

/**
 * Deleting and freeing all the tree node in the binary search tree.
 *
 * This method is the main usage of the new command which mainly reset
 * the word list to original state of. The method recursive deleting all the
 * children nodes in the binary search tree.
 *
 * @param root the root ptr of Node
 */
void deleteTree(Node* root){
    // if the root is null, nothing to delete
    if(root == NULL) return;
    // recursive delete the tree nodes
    deleteTree(root -> left);
    deleteTree(root -> right);
    // delate the root
    delete root;
}


int main()
{
    
    //intiialize
    wordTree->root = NULL;
    
    bool done = false;
    string last;
    while(!done) {
        //promt user
        cout << ">";
        
        //get user input
        string input;
        getline(cin, input);
        transform(input.begin(), input.end(), input.begin(), ::tolower);
        
        
        //fields
        istringstream iss(input);
        bool bad = false;
        vector<string> inputVec;
        string command;
        
        //get rid of white space
        copy(istream_iterator<string>(iss),
             istream_iterator<string>(),
             back_inserter(inputVec));
        
        
        command = inputVec[0];
        
        if (command == "load") {
            
            
            //file
            ifstream infile(inputVec[1].c_str());
            
            if (inputVec.size() != 2) {
                bad = true;
                
            }if(!infile){
                bad = true;
            }if(wordTree->root != NULL && last != "load"){
                bad = true;
            }
            
            if(bad == false){
                
                load(inputVec[1].c_str());
            }
            
        } else if (command == "locate") {
            
            if (inputVec.size() != 3 ) {
                bad = true;
            } else {
                
                //for lookup
                int occurence = atoi(inputVec[2].c_str());
                int i = 1;
                int* pt = &i;
                
                if( locate(wordTree->root, inputVec[1], occurence, pt) == 0){
                    cout << "No matching entry." << endl;
                }else{
                    cout << locate(wordTree->root, inputVec[1], occurence, pt) <<endl;
                }
            }
            
        } else if (command == "new") {
            if (inputVec.size() != 1) {
                bad = true;
            } else {
                
                deleteTree(wordTree->root);
                wordTree->root = NULL;
                
                
            }
            
        } else if (command == "end") {
            if (inputVec.size() != 1) {
                bad = true;
            } else {
                done = true;
                deleteTree(wordTree->root);
            }
        } else {
            bad = true;
        }
        
        
        if (bad) {
            cout << "ERROR: Invalid command" << endl;
        }
        last = inputVec[0];
    }
    
    //free pointers
    delete wordTree;
    
    return 0;
}
