#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

// Define the structure for a node in the linked list
typedef struct Node {
    uint64_t data;
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
uint64_t isEmpty(Queue* q);

// Function to add an element to the queue
void enqueue(Queue* q, uint64_t data);

// Function to remove first element from the queue
uint64_t dequeue(Queue* q);

// Function to remove matched element from the queue
uint64_t dequeue_an_entry(Queue* q, uint64_t data);

// Function to pruint64_t the queue
void print_tQueue(Queue* q);

