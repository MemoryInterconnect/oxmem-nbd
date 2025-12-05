#include "lib-queue.h"

// Function to create a new node with given data
Node* createNode(uint64_t data) {
    Node* newNode = (Node*)malloc(sizeof(Node));
    if (!newNode) return NULL;
    
    newNode->data = data;
    newNode->next = NULL;
    return newNode;
}

// Function to initialize the queue
void initQueue(Queue* q) {
    q->front = q->rear = NULL;
    pthread_mutex_init(&q->lock, NULL);
}

// Function to check if the queue is empty
int isEmpty(Queue* q) {
    return q->front == NULL;
}

// Function to add an element to the queue
void enqueue(Queue* q, uint64_t data) {
    Node* newNode = createNode(data);
    if (!newNode) {
        printf("Memory allocation failed!\n");
        return;
    }

    pthread_mutex_lock(&q->lock);

    if (isEmpty(q)) {
        q->front = q->rear = newNode;
    } else {
        q->rear->next = newNode;
        q->rear = newNode;
    }
    pthread_mutex_unlock(&q->lock);
}

// Function to remove an element from the queue
uint64_t dequeue(Queue* q) {
    pthread_mutex_lock(&q->lock);
    if (isEmpty(q)) {
        pthread_mutex_unlock(&q->lock);
        return 0;
    }

    Node* temp = q->front;
    uint64_t data = temp->data;
    q->front = q->front->next;

    if (q->front == NULL) {
        q->rear = NULL;
    }
    pthread_mutex_unlock(&q->lock);

    free(temp);
    return data;
}

uint64_t dequeue_an_entry(Queue* q, uint64_t data) {
    Node * prev = NULL;
    Node * temp = NULL;

    pthread_mutex_lock(&q->lock);
    if (isEmpty(q)) {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

    temp = q->front;
    while(temp) {
        if ( temp->data == data ) break;
        prev = temp;
        temp = temp->next;
    }

    if ( temp == NULL ) {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

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
    pthread_mutex_unlock(&q->lock);

    free(temp);
    return data;
}


// Function to print the queue
void printQueue(Queue* q) {
    Node* temp = q->front;
    while (temp) {
        printf("%lu ", temp->data);
        temp = temp->next;
    }
    printf("\n");
}

