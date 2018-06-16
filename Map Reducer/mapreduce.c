#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/sysinfo.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "mapreduce.h"
#include <assert.h>

//int *partition_array = 0;
int file_num = 1;
int tot_file_num = 1;
int num_partitions = 0;
Mapper mapper = NULL;
Reducer reducer = NULL;
Partitioner partitioner = NULL;
char* get_next(char *key, int partition_number);

typedef struct node {
  char* key;
  char* value;
  struct LIST *values;
  struct node * next;
} Node, K;

typedef struct LIST {
  Node *head;
  Node *tail;
  pthread_mutex_t p_lock;
} List;

List *partition_array;

pthread_mutex_t map_lock = PTHREAD_MUTEX_INITIALIZER;

void print() {
  for (int i=0; i<num_partitions; i++) {
    printf("%d\n", i);
    Node *list = (struct node*) partition_array[i].head;
    while(list != NULL) {
      printf("K:%s->", list->key);
      Node *temp = list->values->head;
      while(temp != NULL) {
        printf(",%s", temp->value);
        temp = temp->next;
      }
      list = list->next;
      printf("\n");
    }
    printf("---");
  }
}
void* map_init(void *arg) {
  char** argv = (char**) arg;
  int loc_file_num = 1;
    pthread_mutex_lock(&map_lock);
    while (file_num < tot_file_num) {
//    if (file_num < tot_file_num) {
      loc_file_num = file_num; // Since each thread has its own stack, this is exclusive and hence thread safe
/*    }
    else {
    pthread_mutex_unlock(&map_lock);
    return 0;
  } */
  file_num++;
    pthread_mutex_unlock(&map_lock);
    mapper(argv[loc_file_num]);
  }
  pthread_mutex_unlock(&map_lock);
  return 0;
}

void* reduce_init(void* arg) {
int partition_number = *((int*) arg);
Node *temp = (struct node*) partition_array[partition_number].head;

while(temp != NULL) {
//  printf("%s\n", temp->key);
  reducer(temp->key, &get_next, partition_number);
  Node *next = temp->next;
  free(temp);
  temp = next;
  partition_array[partition_number].head = next;
}
return NULL;
}

void init_array()
{
  int i = 0;
  for (i = 0; i < num_partitions; i++) {
    partition_array[i].head = NULL;
    partition_array[i].tail = NULL;
    //partition_array[i].p_lock = PTHREAD_MUTEX_INITIALIZER;
    int rc = pthread_mutex_init(&(partition_array[i].p_lock), NULL);
    assert(rc == 0);
  }
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition) {
  pthread_mutex_init(&(map_lock), NULL);
  file_num = 1;
  tot_file_num = argc;
  mapper = map;
  reducer = reduce;
  partitioner = partition;
  num_partitions = num_reducers;
  partition_array = malloc(num_reducers*sizeof(List));
  pthread_t map_thread_num[num_mappers];
  init_array();
  for (int i=0; i<num_mappers; i++) {
    pthread_create(&map_thread_num[i], NULL, map_init, argv);
  }
  for (int i=0; i<num_mappers; i++) {
    pthread_join(map_thread_num[i], NULL);
  }
//  print();
  pthread_t reduce_thread_num[num_reducers];
  int num_r[num_reducers];
  for (int i=0; i<num_reducers; i++) {
    num_r[i] = i;
    pthread_create(&reduce_thread_num[i], NULL, reduce_init, (void*) &num_r[i]);
  }
  for (int i=0; i<num_reducers; i++) {
    pthread_join(reduce_thread_num[i], NULL);
  }
  free(partition_array);
}


void MR_Emit(char *key, char *value) {

  int partition_number = partitioner(key, num_partitions); //TODO
  //if (partition_number>=1) partition_number--;

  pthread_mutex_lock(&partition_array[partition_number].p_lock);

  /* Extracting Linked List at a given index */
  Node *list = (struct node*) partition_array[partition_number].head;

  /* Creating an item to insert in the Hash Table */
  Node *new_value_node = (struct node*) malloc(sizeof(struct node));
  new_value_node->value = strdup(value);
  new_value_node->next = NULL;
  struct node *new_KV_pair = (struct node*) malloc(sizeof(struct node));
  new_KV_pair->values = malloc(sizeof(struct LIST));
//  printf("%s\n", key);
  new_KV_pair->key = strdup(key);
  //printf("%p\n", new_KV_pair->values->head);
  //printf("%p\n", new_value_node);
  new_KV_pair->values->head = new_value_node;
//  printf("REACHED1\n");
  new_KV_pair->next = NULL;

  if (list == NULL) {
//    printf("REACHED2\n");
    /* Absence of Linked List at a given Index of Hash Table */
    // Inserting key and value
    int rc = pthread_mutex_init(&(new_KV_pair->values->p_lock), NULL);
    assert(rc == 0);
    partition_array[partition_number].head = new_KV_pair;
    partition_array[partition_number].tail = new_KV_pair;
  }
  else {
//    printf("REACHED3\n");
    /* A Linked List is present at given index of Hash Table */
    /* Adding value to existing list */
    Node *temp = list;
    Node *prev_node = (struct node*) malloc(sizeof(struct node));
    while (temp != NULL)
    {
      //printf("REACHED4\n");
      if (strcmp(temp->key,key) < 0) {
        //printf("REACHED_A\n");
        prev_node = temp;
        temp = temp->next;
        continue;
      }
      /* Key is found */
      else if (strcmp(temp->key,key) == 0) {
        //printf("REACHED_B\n");
        pthread_mutex_lock(&temp->values->p_lock);
        new_value_node->next = temp->values->head;
        temp->values->head = new_value_node;
        pthread_mutex_unlock(&temp->values->p_lock);
        break;
      }
      /* Key not found, inserting in sorted manner */
      else if (strcmp(temp->key,key) > 0) {
        if (temp == partition_array[partition_number].head)
          partition_array[partition_number].head = new_KV_pair;
        new_KV_pair->next = temp;
        int rc = pthread_mutex_init(&(new_KV_pair->values->p_lock), NULL);
        assert(rc == 0);
      //printf("REACHED_C\n");
        if(prev_node->next == NULL)
        prev_node->next = (struct node*) malloc(sizeof(struct node));
      //  if (new_KV_pair == NULL) printf("REACHED_D\n");
        prev_node->next = new_KV_pair;
        break;
      }
      //temp = temp->next;
    }
    if (temp == NULL) {
      /*
      *Key not found in existing linked list
      *Adding the key at the end of the linked list
      */
    //  printf("REACHED5\n");
      int rc = pthread_mutex_init(&(new_KV_pair->values->p_lock), NULL);
      assert(rc == 0);
      partition_array[partition_number].tail->next = new_KV_pair;
      partition_array[partition_number].tail = new_KV_pair;
    }
  }
    pthread_mutex_unlock(&partition_array[partition_number].p_lock);
}

char* get_next(char *key, int partition_number) {
  static pthread_mutex_t ret_value_mutex = PTHREAD_MUTEX_INITIALIZER;
//  printf("REACHED5\n");
  struct node *temp = (struct node*) partition_array[partition_number].head;
  //Node *temp = list;
  static char* ret_value = NULL;

  while(strcmp(temp->key,key) != 0) {
    temp = temp->next;
//    printf("REACHED_A\n");
    if (temp == NULL) break;
  }
  if (temp != NULL) {
//    printf("REACHED_B\n");
//    assert ((temp->values->head->value) != NULL);
    if ((temp->values->head) == NULL) { return NULL; }
    //ret_value = malloc(sizeof(temp->values->head->value));
    //strcpy(ret_value, temp->values->head->value);
    if (pthread_mutex_lock(&ret_value_mutex) == 0) {
//      printf("REACHED_D\n");
    ret_value = temp->values->head->value;
//    printf("REACHED_E\n");
    pthread_mutex_unlock(&ret_value_mutex);
  }

    if (ret_value != NULL) {
//      printf("REACHED_C\n");
      Node *next_head = temp->values->head->next;
      free(temp->values->head);
      temp->values->head = next_head;
    }
  }
//  printf("Returning\n");
  //printf("ret_value:%s\n", ret_value);
  return ret_value;
}


unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
  unsigned long hash = 5381;
  int c;
  while ((c = *key++) != '\0')
  hash = hash * 33 + c;
  return hash % num_partitions;
}
