#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <netdb.h>
#include <unistd.h>
#include <assert.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <pthread.h>

#include <stddef.h>
#include <arpa/inet.h>

#include "libds.h"
#include "libhttp.h"
#include "jansson.h"

const char *HTTP_404_CONTENT = "<html><head><title>404 Not Found</title></head><body><h1>404 Not Found</h1>The requested resource could not be found but may be available again in the future.<div style=\"color: #eeeeee; font-size: 8pt;\">Actually, it probably won't ever be available unless this is showing up because of a bug in your program. :(</div></html>";
const char *HTTP_501_CONTENT = "<html><head><title>501 Not Implemented</title></head><body><h1>501 Not Implemented</h1>The server either does not recognise the request method, or it lacks the ability to fulfill the request.</body></html>";

const char *HTTP_200_STRING = "OK";
const char *HTTP_404_STRING = "Not Found";
const char *HTTP_501_STRING = "Not Implemented";
#define MAX_CONNECTIONS    (10)
#define MAX_LINE           (1000)
#ifndef __QUEUE_H__
#define __QUEUE_H__
/**
 * Queue Data Structure
 */
struct queue_node {
	void *item; ///<Stored value
	struct queue_node *next; ///<Link to next node
};
typedef struct {
	struct queue_node *head; ///<Head of linked-list
	struct queue_node *tail; ///<Tail of linked-list
	unsigned int size; ///<Number of nodes in linked-list
} queue_t;
void queue_init(queue_t *q);
void queue_destroy(queue_t *q);
void *queue_dequeue(queue_t *q);
void *queue_at(queue_t *q, int pos);
void *queue_remove_at(queue_t *q, int pos);
void queue_enqueue(queue_t *q, void *item);
unsigned int queue_size(queue_t *q);
void queue_iterate(queue_t *q, void (*iter_func)(void *, void *), void *arg);
#endif

/****************************/
pthread_t tid[MAX_CONNECTIONS] ;
int terminator_flag=0;
char IParray[INET_ADDRSTRLEN];
queue_t queue_clientfd;
typedef struct 
{
	int status_code;
	int content_length;
	char content_type[20];
	
} response_t;
typedef struct _mapreduce_t
{
	// You will need to add some stuff here! :)
	datastore_t datastore;
 	char revision[32];
 	unsigned long rev;
} mapreduce_t;
mapreduce_t map;
/****************************/
pthread_t tid[10];
void signal_handler();
void * worker(void * client_fd);
char* write_header( response_t response_info, char* connection );
char * server_get(char * pch, const char * statusreq, http_t * http_request, int * client_fd);
void server_delete(char * pch, const char * statusreq, http_t * http_request, int * client_fd);
int sockfd;
/****************************/

int main(int argc, char **argv)
{
		        
        //set signal
		signal( SIGINT, signal_handler );
		//signal( SIGKILL, signal_handler_thread);
	    if (argc != 2)
        {
                fprintf(stderr, "Usage: %s [port number]\n", argv[0]);
                return 1;
        }		
		int port = atoi(argv[1]);
		if (port <= 0 || port >= 65536)
		{
			fprintf(stderr, "Illegal port number.\n");
			return 1;
		}

		datastore_init(&(map.datastore));
		map.revision[0]= '\0';
		map.rev =0;
		sockfd = socket(AF_INET, SOCK_STREAM, 0);
		queue_init(&queue_clientfd);

		// struct sockaddr_in my_addr;
		// my_addr.sin_family = AF_INET;
		// my_addr.sin_addr.s_addr = htonl(INADDR_ANY);
		// my_addr.sin_port = htons(port);
		struct addrinfo hints, *addrinfo;
		memset(&hints,0,sizeof(hints) );
		hints.ai_flags = AI_PASSIVE;
  		hints.ai_family = AF_INET;
  		hints.ai_socktype = SOCK_STREAM; // TCP socket
  		hints.ai_protocol = 0;//tcp
		if (getaddrinfo(  NULL, argv[1], &hints,&addrinfo)  != 0)
		{
			perror("getaddrinfo");
			exit(1);
		}

		int bind_flag = bind(sockfd,addrinfo->ai_addr, addrinfo->ai_addrlen);
		if (bind_flag < 0)
		{
		perror("binding socket");
		exit(1);
		}
		freeaddrinfo(addrinfo);// no need addrinfo

		int listen_flag =  listen(sockfd, MAX_CONNECTIONS);
		if (listen_flag < 0)
		{
			perror("listen");
			exit(2);
		}
		
		pthread_attr_t attr;
    	pthread_attr_init(&attr);
    	pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_DETACHED);
		
		int count = 0;
		fd_set master, slave;
		int terminator_fd=0;
		FD_ZERO( &master );
		FD_ZERO( &slave );
		FD_SET( sockfd, &master );
		FD_SET( terminator_fd, &master );
	  	while(1){
	  		slave = master;
			struct sockaddr_in client;
			socklen_t client_len = sizeof(client);
			

			struct timeval t2sec;
				  t2sec.tv_sec = 2;
				  t2sec.tv_usec = 0;
			//int retval = select(sockfd+1, &slave, NULL, NULL, &t2sec );
			int retval = select(FD_SETSIZE, &slave, NULL, NULL, &t2sec );
			if (terminator_flag ==1)
			{
				break;
			}
			if (retval==-1|| retval == 0)
			{
				continue;
			}
			int * client_fd = (int *)malloc(sizeof(int));
			*client_fd = accept(sockfd, (struct sockaddr *) &client,  &client_len);
			//printf("A NEW CLIENT FD%d\n", *client_fd);
			if (*client_fd < 0)
			{
				free(client_fd);
				continue;
			}
			//printf("server gets connection with %d\n", *client_fd);

		
			//int thread_flag = pthread_create(&tid[count], &attr,worker, (void *)(client_fd) );
			int thread_flag = pthread_create(&tid[count], NULL,worker, (void *)(client_fd) );
			if (thread_flag != 0)
			{
				close(*client_fd);
				perror("thread create");
				exit(1);
			}
			//pthread_detach(tid[count]);///-----------?
			count++;

			queue_enqueue( &queue_clientfd, client_fd  );
			
	    	
	  	}
	  	int i;
	  	for (i = 0; i < count; ++i)
		{
			pthread_join(tid[i] , NULL);
		}

	  	//close(sockfd); 
	  	queue_destroy(&queue_clientfd);
		
		return 0;
}
void * worker(void * client_fd_pass)
{
	sigset_t mask;
	sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
	pthread_sigmask(SIG_SETMASK, &mask, NULL);
	//int * client_fd = (int*)(client_fd_pass);
	int client_fd = *(int*)(client_fd_pass);

	int alive =1;
		while(alive)
			{
			  
				int res;
				if (terminator_flag == 1 )
				{
					break;
				}
				http_t http_request;
				fd_set master;
				FD_ZERO(&master); 
				FD_SET(client_fd, &master);
				struct timeval t2sec;
				t2sec.tv_sec = 2;
				t2sec.tv_usec=0;
				//int ret = select(*client_fd + 1, &master, NULL, NULL, &t2sec);
				int ret = select(FD_SETSIZE, &master, NULL, NULL, &t2sec);
				if (ret==-1 || ret==0)
				{
					//free(http_request);
				continue;//break;// nothing to receive
				}
				res = http_read( &http_request, client_fd );
				if(res <= 0)
				{
				//free(http_request);
				continue;//break;// nothing to receive
				}

				//set response
				response_t response_info;
				response_info.status_code = 200;

				// // check request file
				// char * filename_return=NULL;
				const char * statusreq = http_get_status(&http_request);
				printf("%s\n", statusreq); 
				char * pch = NULL;
				if ( (pch = strstr(statusreq, "PUT")) != NULL )
				{
					char key[1024];key[0] = '\0';
					pch = strtok(statusreq, " /");
					pch = strtok(NULL, " /");
					strcpy(key, pch);
					size_t contentlength = atoi( http_get_header(&http_request, "Content-Length") );
					const char * bodyres = http_get_body(&http_request,  &contentlength );

					json_error_t error;  
					json_t *body = json_loads( bodyres, 0, &error );
					json_t * value = json_object_get(body, "Value");
					json_t * _rev = json_object_get(body, "_rev");

					unsigned long ret;
					if (_rev != NULL)
					{
						strcpy(map.revision,json_string_value( _rev ));
						ret = datastore_update(&(map.datastore), key, json_string_value(value), atoi(map.revision));
						if (ret==0)
						{
							//did not match
							char buffer[1000];
							sprintf(buffer, "HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: 57\r\nConnection: keep-alive\r\n\r\n");
							send(client_fd, buffer, strlen(buffer), 0);
							json_t * putobject = json_object();
							json_object_set_new(putobject,"error", json_string("conflict") );
							json_object_set_new(putobject,"reason", json_string("Document update conflict.") );
							char * objstr = json_dumps(putobject,0);
							send(client_fd, objstr, 57, 0);
							printf("put send:%s\n", objstr);
							json_decref(putobject);
							free(objstr);
						}
						else
						{
						
							json_t * putobject = json_object();
							char revstr[10];
							sprintf(revstr, "%lu", ret);
							json_object_set_new(putobject,"ok", json_boolean(1) );
							json_object_set_new(putobject,"rev", json_string(revstr) );//_rev
							json_object_set_new(putobject,"id", json_string(key) );
							char * objstr = json_dumps(putobject,0);
							char buffer[1000];
							sprintf(buffer, "HTTP/1.1 201 Created\r\nEtag: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: keep-alive\r\n\r\n", json_string_value(_rev), (int)strlen(objstr));
							send(client_fd, buffer, strlen(buffer), 0);
							send(client_fd, objstr, strlen(objstr), 0);
							printf("update send:%s\n", objstr);
							json_decref(putobject);
							free(objstr);
						}
					}
					else
					{
						ret = datastore_put(&(map.datastore), key, json_string_value(value));
					
						if (ret==0)
						{
							//already exist
							char buffer[1000];
							sprintf(buffer, "HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: 57\r\nConnection: keep-alive\r\n\r\n");
							send(client_fd, buffer, strlen(buffer), 0);
							json_t * putobject = json_object();
							json_object_set_new(putobject,"error", json_string("conflict") );
							json_object_set_new(putobject,"reason", json_string("Document update conflict.") );
							char * objstr = json_dumps(putobject,0);
							send(client_fd, objstr, 57, 0);
							printf("put send:%s\n", objstr);
							json_decref(putobject);
							free(objstr);
						}
						else
						{
						
							json_t * putobject = json_object();
							char revstr[10];
							sprintf(revstr, "%lu", ret);
							json_object_set_new(putobject,"ok", json_boolean(1) );
							json_object_set_new(putobject,"rev", json_string(revstr) );//_rev
							json_object_set_new(putobject,"id", json_string(key) );
							char * objstr = json_dumps(putobject,0);
							char buffer[1000];
							sprintf(buffer, "HTTP/1.1 201 Created\r\nEtag: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: keep-alive\r\n\r\n", json_string_value(_rev), (int)strlen(objstr));
							send(client_fd, buffer, strlen(buffer), 0);
							send(client_fd, objstr, strlen(objstr), 0);
							printf("update send:%s\n", objstr);
							json_decref(putobject);
							free(objstr);
						}	
						
					}
					
				}
				
				else if ( (pch = strstr(statusreq, "GET")) != NULL )
				{
					server_get(pch, statusreq, &http_request, &client_fd);
				}
				else if ( (pch = strstr(statusreq, "DELETE")) != NULL )
				{
					server_delete(pch, statusreq, &http_request, &client_fd);
				}		
				

				 
			}
			printf("go out of while\n");


		
	return NULL;
	//pthread_exit( client_fd_pass  );
}
void server_delete(char * pch, const char * statusreq, http_t * http_request, int * client_fd)
{
	char key[1024];key[0] = '\0';
	pch = strtok(statusreq, " /");
	pch = strtok(NULL, " /");
	strcpy(key, pch);
	printf("key is:%s!!\n", key);
	size_t contentlength = atoi( http_get_header(&http_request, "Content-Length") );
	const char * bodyres = http_get_body(&http_request,  &contentlength );
	json_error_t error;  
	json_t *body = json_loads( bodyres, 0, &error );
	json_t * _rev = json_object_get(body, "If-Match");
	// const char * str_rev = json_string_value( _rev );
	// unsigned long revision = atoi(str_rev);
	unsigned long ret = datastore_delete(&(map.datastore), key, atoi(json_object_get(body, "If-Match")));
	printf("get revision is:%s\n", ret);
	if (ret==0)
	{
		//the key not exit
		char buffer[1000];buffer[0] = '\0';
		sprintf(buffer, "HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: 64\r\nConnection: keep-alive\r\n\r\n");
		printf("buffer is:%s\n", buffer);
		send(*client_fd, buffer, strlen(buffer), 0);
		json_t * putobject = json_object();
		json_object_set_new(putobject,"error", json_string("conflict") );
		json_object_set_new(putobject,"reason", json_string("Revision number was out of date.") );
		char * objstr = json_dumps(putobject,0);
		printf("delete send:%s\n", objstr);
		send(*client_fd, objstr, 64, 0);
		
		json_decref(putobject);
		free(objstr);
	}
	else
	{
		json_t * putobject = json_object();
		json_object_set_new(putobject,"ok", json_boolean(1) );
		json_object_set_new(putobject,"rev", json_integer(ret) );
		char * objstr = json_dumps(putobject,0);
		char buffer[1000];
		sprintf(buffer, "HTTP/1.1 201 Created\r\nEtag: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: keep-alive\r\n\r\n", json_string_value(json_object_get(putobject, "rev")), (int)strlen(objstr));
		send(*client_fd, buffer, strlen(buffer), 0);
		send(*client_fd, objstr, strlen(objstr), 0);
		printf("delete send:%s\n", objstr);
		json_decref(putobject);
		free(objstr);
	}
}
char * server_get(char * pch, const char * statusreq, http_t * http_request, int * client_fd)
{
		char key[1024];key[0] = '\0';
		pch = strtok(statusreq, " /");
		pch = strtok(NULL, " /");
		strcpy(key, pch);
		printf("key is:%s!!\n", key);
		const char * value = datastore_get(&(map.datastore), key, &(map.rev));
		printf("map.rev is:%i\n", map.rev);
		sprintf(map.revision, "%d", map.rev);
		printf("get value is:%s\n", value);
		if (value==NULL)
		{
			//the key not exit
			char buffer[1000];buffer[0] = '\0';
			sprintf(buffer, "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: 72\r\nConnection: keep-alive\r\n\r\n");
			printf("buffer is:%s\n", buffer);
			send(*client_fd, buffer, strlen(buffer), 0);
			json_t * putobject = json_object();
			json_object_set_new(putobject,"error", json_string("not found") );
			json_object_set_new(putobject,"reason", json_string("The key does not exist in the database.") );
			char * objstr = json_dumps(putobject,0);
			printf("get send:%s\n", objstr);
			send(*client_fd, objstr, 73, 0);
			
			json_decref(putobject);
			free(objstr);
		}
		else
		{
			json_t * putobject = json_object();
			json_object_set_new(putobject,"_id", json_string(key) );
			json_object_set_new(putobject,"_rev", json_string(map.revision) );
			json_object_set_new(putobject,"Value", json_string(value) );
			char * objstr = json_dumps(putobject,0);
			char buffer[1000];
			sprintf(buffer, "HTTP/1.1 200 OK\r\nEtag: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: keep-alive\r\n\r\n", json_string_value(json_object_get(putobject, "_rev")), (int)strlen(objstr));
			send(*client_fd, buffer, strlen(buffer), 0);
			send(*client_fd, objstr, strlen(objstr), 0);
			printf("get send:%s\n", objstr);
			json_decref(putobject);

			free(objstr);
		}
		
	return NULL;
}
char* write_header( response_t response_info, char* connection )
{
	char * response_string;
	switch(response_info.status_code)
	{
		case 404:
			response_string = HTTP_404_STRING;
			break;
		case 501:
			response_string = HTTP_501_STRING;
			break;
		case 200:
			response_string = HTTP_200_STRING;
			break;
	}
	return NULL;
}
void signal_handler()
{
	terminator_flag =1;
	unsigned int size_queue = queue_size( &queue_clientfd );
	//printf(" the sizeof the queue is :%i\n", size_queue);
	int i=0;
	for (i = 0; i < (int)size_queue; ++i)
	{
		int * current_client_fd = queue_at( &queue_clientfd, i );
		int ret_close = close(*current_client_fd);
		//printf("%d\n", ret_close);
		free(current_client_fd);
	}
	close(sockfd);
	//exit(0);

}
/*queue data function*/
void queue_init(queue_t *q) {

	q->head = NULL;
	q->tail = NULL;
	q->size = 0;
}

/**
 * Frees all associated memory.
 * Should always be called last.
 *
 * @param q A pointer to the queue data structure.
 * @return void
 */
void queue_destroy(queue_t *q) {
	while(queue_size(q) > 0) {
		queue_dequeue(q);
	}
}

/**
 * Removes and returns element from front of queue.
 * 
 * @param q A pointer to the queue data structure.
 * @return A pointer to the oldest element in the queue.
 * @return NULL if the queue is empty.
 */
void *queue_dequeue(queue_t *q) {
	struct queue_node *front;
	void *item;

	if(queue_size(q) == 0) {
		return NULL;
	}

	front = q->head;
	q->head = q->head->next;
	q->size--;

	item = front->item;
	free(front);

	if(queue_size(q) == 0) {
		// just cleaning up
		q->head = NULL;
		q->tail = NULL;
	}

	return item;
}


/**
 * Removes and returns element at position pos.
 *
 * @param q A pointer to the queue data structure.
 * @param pos Position to be removed.
 * @return A pointer to the element at position pos.
 * @return NULL if the position is invalid.
 */
void *queue_remove_at(queue_t *q, int pos){
	if( pos < 0 || q->size-1 < (unsigned int)pos)
		return NULL;
	q->size--;
	struct queue_node *cur = q->head;
	struct queue_node *prev = NULL;
	while(pos--){
		prev = cur;
		cur = cur->next;
	}

	if(cur == q->head){
		q->head = cur->next;
		if(q->head == NULL)
			q->tail = NULL;
		void *item = cur->item;
		free(cur);
		return item;
	}else if(cur == q->tail){
		q->tail = prev;
		prev->next = NULL;
		void *item = cur->item;
		free(cur);
		return item;
	}else{
		prev->next = cur->next;
		void *item = cur->item;
		free(cur);
		return item;
	}
}

/**
 * Returns element located at position pos.
 *
 * @param q A pointer to the queue data structure.
 * @param pos Zero-based index of element to return.
 * @return A pointer to the element at position pos.
 * @return NULL if position out of bounds.
 */
void *queue_at(queue_t *q, int pos){
	int i;
	struct queue_node *node;
	if(q == NULL)
		return NULL;
		

	for(i=0, node=q->head; i<pos && node != NULL; i++) node=node->next;
	if(i != pos)
		return 0;
	else
		return node->item;

}

/**
 * Stores item at the back of the queue.
 *
 * @param q A pointer to the queue data structure.
 * @param item Value of item to be stored.
 * @return void
 */
void queue_enqueue(queue_t *q, void *item) {
	struct queue_node *back = malloc(sizeof(struct queue_node));

	back->item = item;
	back->next = NULL;
	if(queue_size(q) == 0) {
		q->head = back;
	} else {
		q->tail->next = back;
	}
	q->tail = back;
	q->size++;
}

/**
 * Returns number of items in the queue.
 *
 * @param q A pointer to the queue data structure.
 * @return The number of items in the queue.
 */
unsigned int queue_size(queue_t *q) {
	return q->size;
}

/**
 * Helper function to apply operation on each item.
 *
 * @param q A pointer to the queue data structure.
 * @param iter_func Function pointer to operation to be applied.
 * @param arg Pass through variable to iter_func.
 * @return void
 */
void queue_iterate(queue_t *q, void (*iter_func)(void *, void *), void *arg) {
	struct queue_node *node;
	if(queue_size(q) == 0) {
		return;
	}

	node = q->head;
	while(node != NULL) {
		iter_func(node->item, arg);
		node = node->next;
	}
}