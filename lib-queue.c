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

// Function to create a new node with given data
Node* createNode(int data) {
    Node* newNode = (Node*)malloc(sizeof(Node));
    if (!newNode) return NULL;
    
    newNode->data = data;
    newNode->next = NULL;
    return newNode;
}

// Function to initialize the queue
void initQueue(Queue* q) {
    q->front = q->rear = NULL;
}

// Function to check if the queue is empty
int isEmpty(Queue* q) {
    return q->front == NULL;
}

// Function to add an element to the queue
void enqueue(Queue* q, int data) {
    Node* newNode = createNode(data);
    if (!newNode) {
        printf("Memory allocation failed!\n");
        return;
    }
    
    if (isEmpty(q)) {
        q->front = q->rear = newNode;
    } else {
        q->rear->next = newNode;
        q->rear = newNode;
    }
}

// Function to remove an element from the queue
int dequeue(Queue* q) {
    if (isEmpty(q)) {
//        printf("Queue is empty!\n");
        return -1;
    }
    
    Node* temp = q->front;
    int data = temp->data;
    q->front = q->front->next;
    
    if (q->front == NULL) {
        q->rear = NULL;
    }
    
    free(temp);
    return data;
}

int dequeue_an_entry(Queue* q, int data) {
    Node * prev = NULL;
    Node * temp = NULL;

    if (isEmpty(q)) {
//        printf("Queue is empty!\n");
        return -1;
    }
    
    temp = q->front;
    while(temp) {
        if ( temp->data == data ) break;
        prev = temp;
        temp = temp->next;
    }

    if ( temp == NULL ) return -1;

    if ( temp == q->front ) {
	q->front = temp->next;
    }
	    
    if ( temp == q->rear ) {
	q->rear = prev;
    }

    if ( prev ) {
	prev->next = temp->next;
    }
    
    if (q->front == NULL) {
        q->rear = NULL;
    }
    
    free(temp);
    return data;
}


// Function to print the queue
void printQueue(Queue* q) {
    Node* temp = q->front;
    while (temp) {
        printf("%d ", temp->data);
        temp = temp->next;
    }
    printf("\n");
}

