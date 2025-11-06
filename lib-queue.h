#include <stdio.h>
#include <stdlib.h>

// Define the structure for a node in the linked list
typedef struct Node {
    int data;
    struct Node* next;
} Node;

// Define the queue structure with pointers to the front and rear nodes
typedef struct Queue {
    Node* front;
    Node* rear;
} Queue;

// Function to initialize the queue
void initQueue(Queue* q); 

// Function to check if the queue is empty
int isEmpty(Queue* q);

// Function to add an element to the queue
void enqueue(Queue* q, int data);

// Function to remove first element from the queue
int dequeue(Queue* q);

// Function to remove matched element from the queue
int dequeue_an_entry(Queue* q, int data);

// Function to print the queue
void printQueue(Queue* q);

