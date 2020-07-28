#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <curl/multi.h> /* multi curl */
#include <libxml/HTMLparser.h>
#include <libxml/parser.h>
#include <libxml/xpath.h>
#include <libxml/uri.h>
#include <search.h> /* for hash table */
#include <time.h>
#include <sys/time.h>

#define BUF_SIZE 1048576  /* 1024*1024 = 1M */
#define BUF_INC  524288   /* 1024*512  = 0.5M */

#define CT_PNG "image/png"
#define CT_HTML "text/html"
#define CT_PNG_LEN 9
#define CT_HTML_LEN 9


#define max(a, b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

typedef struct recv_buf2 {
    char *buf;       /* memory to hold a copy of received data */
    size_t size;     /* size of valid data in buf in bytes*/
    size_t max_size; /* max capacity of buf in bytes*/
    int seq;         /* >=0 sequence number extracted from http header */
                     /* <0 indicates an invalid seq number */
} RECV_BUF;

typedef struct url_node2 {
    char url[256];
    struct url_node2 *next;
} url_node;

typedef struct {
    CURL *curl;
    RECV_BUF *buf;
    char url[256];
} connection;

/* global variables */
int png_num;
int conc_num;
char* log_file;
url_node *png_list;
url_node *frontier_list;
struct hsearch_data *htab;
CURLM *cm = NULL;
connection *curl_handles;

/* declarations */
htmlDocPtr mem_getdoc(char *buf, int size, const char *url);
xmlXPathObjectPtr getnodeset (xmlDocPtr doc, xmlChar *xpath);
size_t header_cb_curl(char *p_recv, size_t size, size_t nmemb, void *userdata);
size_t write_cb_curl3(char *p_recv, size_t size, size_t nmemb, void *p_userdata);
int recv_buf_init(RECV_BUF *ptr, size_t max_size);
int recv_buf_cleanup(RECV_BUF *ptr);
void cleanup();
void easy_handle_init(CURL *curl_handle, RECV_BUF *ptr, char *url);
int process_data(CURL *curl_handle, RECV_BUF *p_recv_buf, char *url);
int find_http(CURL* curl_handle, RECV_BUF *p_recv_buf, const char *base_url);
int process_png(CURL *curl_handle, RECV_BUF *p_recv_buf, char *url);
int process_html(CURL *curl_handle, RECV_BUF *p_recv_buf, char *url);
void init_url(url_node *url, char* data);
void web_crawl();
void write_png();
void write_log(char* file_name, char* url);
int is_png(RECV_BUF *data);
void free_list(url_node *list);
void add_visited(char *url);
void cleanup_handles();
int find_connection(CURL* url);
void get_url();

/* stack helper functions */
void push_frontiers(char *href);
void pop_frontiers();

/* main */
int main(int argc, char** argv) {
	char url[256];
    int c;
    conc_num = 1; /* max num of concurrent connections */
    png_num = 50; /* number of unique png files to find */
    log_file = NULL; /* name of log file */

    if (argc == 1) {
        printf( "Usage: %s [OPTION] SEED_URL\n", argv[0] );
        return -1;
    }

	/* parse command line arguments */
    while ((c = getopt (argc, argv, "t:m:v:")) != -1) {
        switch (c) {
        case 't':
			/* convert string to an unsigned long int */
            conc_num = strtoul(optarg, NULL, 10);
            if (conc_num < 1) {
                printf("%s: conc_num error - max number of concurrent connections cannot be less than 1\n", argv[0]);
                return EXIT_FAILURE;
            }
            break;
        case 'm':
            png_num = strtoul(optarg, NULL, 10);
            if (png_num < 0) {
                printf("%s: png_num error - the number of unique png urls cannot be less than 0\n", argv[0]);
                return EXIT_FAILURE;
            }
            break;
        case 'v':
            log_file = optarg;
            break;
        default:
            break;
        }
    }

    if (optind == argc) {
        printf( "Usage: %s [OPTION] SEED_URL\n", argv[0] );
        return -1;
    }

    if (optind < argc){
		strcpy(url, argv[optind]);
	}

    /* initialize global lists */
    png_list = malloc(sizeof(url_node)); /* list of valid urls */
    frontier_list = malloc(sizeof(url_node)); /* list of urls to visit */
    htab = calloc(1, sizeof(struct hsearch_data)); /* list of visited urls */
    init_url(png_list, NULL);
    init_url(frontier_list, url);
    hcreate_r(1000, htab); 

    /* setup multi curl */
	curl_global_init(CURL_GLOBAL_ALL);
    cm = curl_multi_init();
    curl_handles = malloc(sizeof(connection)*conc_num);
    for (int i = 0; i < conc_num; i++) {
        curl_handles[i].url[0] = '\0';
    }

    /* create log file */
    if (log_file != NULL){
        FILE *fp = fopen(log_file, "w");
        fclose(fp);
    }

    /* start timer */
	struct timeval tv;
	double times[2];
	if (gettimeofday(&tv,NULL) != 0) {
			perror("gettimeofday");
			abort();
	}
	times[0] = (tv.tv_sec) + tv.tv_usec/1000000.;
    
    /* search for pngs */
    web_crawl();

    /* write png urls to file */
    write_png();

    /* end timer */
    if (gettimeofday(&tv,NULL) != 0) {
        perror("gettimeofday");
        abort();
    }
    times[1] = (tv.tv_sec) + tv.tv_usec/1000000.;
    printf("findpng3 execution time: %.6lf seconds\n", times[1] - times[0]);

	/* clean up */
    xmlCleanupParser();
    cleanup();
	return 0;
}

/* API */
/* parse an XML in-memory document and build a tree using read memory */
htmlDocPtr mem_getdoc (char *buf, int size, const char *url) {
	int opts = HTML_PARSE_NOBLANKS | HTML_PARSE_NOERROR | \
               HTML_PARSE_NOWARNING | HTML_PARSE_NONET;
    htmlDocPtr doc = htmlReadMemory(buf, size, url, NULL, opts);
    
    if ( doc == NULL ) {
        /* fprintf(stderr, "Document not parsed successfully.\n"); */
        return NULL;
    }
    return doc;
}

xmlXPathObjectPtr getnodeset (xmlDocPtr doc, xmlChar *xpath) {	
    xmlXPathContextPtr context;
    xmlXPathObjectPtr result;

	/* create a new XML context */
    context = xmlXPathNewContext(doc);
    if (context == NULL) {
        printf("Error in xmlXPathNewContext\n");
        return NULL;
    }

	/* evaluate the XPath Location Path in the given context */
    result = xmlXPathEvalExpression(xpath, context);

	/* free up the context */
    xmlXPathFreeContext(context);

    if (result == NULL) {
        printf("Error in xmlXPathEvalExpression\n");
        return NULL;
    }
    if(xmlXPathNodeSetIsEmpty(result->nodesetval)){
        xmlXPathFreeObject(result);
        /* printf("No result\n"); */
        return NULL;
    }
    return result;
}

void push_frontiers(char *href) {
    url_node *push_url = calloc(1,sizeof(url_node));
    init_url(push_url, href);
    push_url->next = frontier_list;
    frontier_list = push_url; /* set new url as the head of the frontier stack */
    printf("%s is pushed onto the frontier stack\n", frontier_list->url);
}

void pop_frontiers() {
    url_node *temp = frontier_list;
    frontier_list = frontier_list->next;
	free(temp);
}

void add_visited(char *url) {
	ENTRY *item = calloc(1,sizeof(ENTRY));
	ENTRY *retval = item;
    item->key = strdup(url);
    item->data = NULL;
    hsearch_r(*item,ENTER,&retval,htab);
    /* printf("item added to the visited hash table %s\n", item->key); */
    if (log_file != NULL) {
        write_log(log_file, item->key);
    }
}

int find_http(CURL* curl_handle, RECV_BUF *p_recv_buf, const char *base_url)
{
    int i;
    htmlDocPtr doc;
    xmlChar *xpath = (xmlChar*) "//a/@href";
    xmlNodeSetPtr nodeset;
    xmlXPathObjectPtr result;
    xmlChar *href;
    ENTRY item;
    ENTRY *retval;
	char *buf = p_recv_buf->buf;
	int size = p_recv_buf->size;

    if (buf == NULL) {
        return 1;
    }

	/* call mem_getdoc to parse the XML */
	doc = mem_getdoc(buf, size, base_url);
	
	/* evaluate the Xpath location path */
    result = getnodeset(doc, xpath);

    if (result) {
        nodeset = result->nodesetval;

		/* traverse through the number of node */
        for (i=0; i < nodeset->nodeNr; i++) {

			/* get the href */
            href = xmlNodeListGetString(doc, nodeset->nodeTab[i]->xmlChildrenNode, 1);

            if ( href != NULL && !strncmp((const char *)href, "http", 4) ) {
                /* printf("href is %s\n", href); */
                char *new_href = strdup((char*)href);
                item.key = new_href;
                item.data = NULL;
                int success;
                success = hsearch_r(item,FIND,&retval,htab);
                /* check if we have not visited this webpage or png */
                if (success == 0) {
                    push_frontiers(new_href);
                }
                free(new_href);
				xmlFree(href);	
			}
		}
		xmlXPathFreeObject(result);
    }
    xmlFreeDoc(doc);
    return 0;
}

/* header callback to extract image sequence number */
size_t header_cb_curl(char *p_recv, size_t size, size_t nmemb, void *userdata) {
    int realsize = size * nmemb;
    RECV_BUF *p = userdata;

#ifdef DEBUG1_
    /* printf("%s", p_recv); */
#endif /* DEBUG1_ */
    if (realsize > strlen(ECE252_HEADER) &&
	strncmp(p_recv, ECE252_HEADER, strlen(ECE252_HEADER)) == 0) {
    	/* extract img sequence number */
		p->seq = atoi(p_recv + strlen(ECE252_HEADER));
    }
    return realsize;
}

/* write callback to save a copy of received data in RAM */
size_t write_cb_curl3(char *p_recv, size_t size, size_t nmemb, void *p_userdata) {
    size_t realsize = size * nmemb; 
    RECV_BUF *p = (RECV_BUF *)p_userdata; /* save a copy of the received data */
 
    if (p->size + realsize + 1 > p->max_size) {/* hope this rarely happens */ 
        /* received data is not 0 terminated, add one byte for terminating 0 */
        size_t new_size = p->max_size + max(BUF_INC, realsize + 1);   
        char *q = realloc(p->buf, new_size);
        if (q == NULL) {
            perror("realloc"); /* out of memory */
            return -1;
        }
		/* reassign p->buf and p->max_size */
        p->buf = q;
        p->max_size = new_size;
    }

    memcpy(p->buf + p->size, p_recv, realsize); /*copy data from libcurl*/
    p->size += realsize;
    p->buf[p->size] = 0;

    return realsize;
}

int recv_buf_init(RECV_BUF *ptr, size_t max_size) {
    void *p = NULL;
    if (ptr == NULL) return 1;
    p = malloc(max_size);
    if (p == NULL) return 2;

    ptr->buf = p;
    ptr->size = 0;
    ptr->max_size = max_size;
    ptr->seq = -1; /* valid seq should be positive */
    return 0;
}

int recv_buf_cleanup(RECV_BUF *ptr) {
    if (ptr == NULL || ptr->buf == NULL) return 1;
    free(ptr->buf);
    ptr->buf = NULL;
    ptr->size = 0;
    ptr->max_size = 0;
    return 0;
}

void cleanup() {
    cleanup_handles();
    curl_multi_cleanup(cm);
	curl_global_cleanup();
    free_list(png_list);
    free_list(frontier_list);
	hdestroy_r(htab);
	free(htab);
}

void free_list(url_node *list) { 
    url_node *temp;
    while (list != NULL) { 
        memset(list->url, 0, sizeof(list->url));
        temp = list;
        list = list->next;
        free(temp);
    }
}

void cleanup_handles(){
    for (int i = 0; i < conc_num; i++){
        if (curl_handles[i].url[0] != '\0'){
            recv_buf_cleanup(curl_handles[i].buf);
            curl_multi_remove_handle(cm, curl_handles[i].curl);
            curl_easy_cleanup(curl_handles[i].curl);
        }
    }
    free(curl_handles);
}

/**
 * @brief create a curl easy handle and set the options.
 * @param RECV_BUF *ptr points to user data needed by the curl write call back function
 * @param const char *url is the target url to fetch resoruce
 * @return a valid CURL * handle upon sucess; NULL otherwise
 * Note: the caller is responsbile for cleaning the returned curl handle
 */

void easy_handle_init(CURL* curl_handle, RECV_BUF *ptr, char *url) {

    if (curl_handle == NULL) {
        fprintf(stderr, "curl_easy_init: returned NULL\n");
        return;
    }

    /* register write call back function to process received data */
    curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, write_cb_curl3); 
    /* user defined data structure passed to the call back function */
    curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)ptr);

    /* register header call back function to process received header data */
    curl_easy_setopt(curl_handle, CURLOPT_HEADERFUNCTION, header_cb_curl); 
    /* user defined data structure passed to the call back function */
    curl_easy_setopt(curl_handle, CURLOPT_HEADERDATA, (void *)ptr);

	/* some servers requires a user-agent field */
    curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "ece252 lab5 crawler");

    /* follow HTTP 3XX redirects */
    curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L);
    /* continue to send authentication credentials when following locations */
    curl_easy_setopt(curl_handle, CURLOPT_UNRESTRICTED_AUTH, 1L);
    /* max number of redirects to follow sets to 5 */
    curl_easy_setopt(curl_handle, CURLOPT_MAXREDIRS, 5L);
    /* supports all built-in encodings */
    curl_easy_setopt(curl_handle, CURLOPT_ACCEPT_ENCODING, "");

	/* Enable the cookie engine without reading any initial cookies */
    curl_easy_setopt(curl_handle, CURLOPT_COOKIEFILE, "");
    /* allow whatever auth the proxy speaks */
    curl_easy_setopt(curl_handle, CURLOPT_PROXYAUTH, CURLAUTH_ANY);
    /* allow whatever auth the server speaks */
    curl_easy_setopt(curl_handle, CURLOPT_HTTPAUTH, CURLAUTH_ANY);

    curl_easy_setopt(curl_handle, CURLOPT_HEADER, 0L);
    curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 0L);

    /* specify URL to get */
    curl_easy_setopt(curl_handle, CURLOPT_URL, url);
    curl_easy_setopt(curl_handle, CURLOPT_PRIVATE, url);

    /* add handle */
    curl_multi_add_handle(cm, curl_handle);
}

int process_html(CURL *curl_handle, RECV_BUF *p_recv_buf, char *url) {
    char *eurl = NULL; 
    curl_easy_getinfo(curl_handle, CURLINFO_EFFECTIVE_URL, &eurl);

    /* check if redirected url has already been visited */
    ENTRY item;
    ENTRY *retval = &item;
    item.key = eurl;
    item.data = NULL;
    int url_exists;
    url_exists = hsearch_r(item, FIND, &retval, htab);
    if (!url_exists) {
        add_visited(url);
        find_http(curl_handle, p_recv_buf, url);
		/* the url could have been redirected and not equal to eurl */
        if (strcmp(url, eurl) != 0) {
            add_visited(eurl);
        }
    }
    return 0;
}

int process_png(CURL *curl_handle, RECV_BUF *p_recv_buf, char *url) {
    if (is_png(p_recv_buf) == 0) {
        add_visited(url);
        return 1;
    }
    char *eurl = NULL;
    curl_easy_getinfo(curl_handle, CURLINFO_EFFECTIVE_URL, &eurl);
    ENTRY item;
    ENTRY *retval = &item;
    item.key = eurl;
    item.data = NULL;
    int url_exists;
    url_exists = hsearch_r(item, FIND, &retval, htab);
    if (!url_exists) {
		add_visited(url);
		/* add it to a singly linked list of urls */
        url_node *new_png = malloc(sizeof(url_node));
        init_url(new_png, eurl);
        new_png->next = png_list;
        png_list = new_png;
        png_num--;
        if (strcmp(url, eurl) != 0) {
            add_visited(eurl);
        }
    }
    return 0;
}

int process_data(CURL *curl_handle, RECV_BUF *p_recv_buf, char *url) {
    CURLcode res;
    long response_code;

    res = curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &response_code);

    /* don't process if status code is 4xx or 5xx */
    if (res != CURLE_OK || response_code >= 400) {
        add_visited(url);
        return 1;
    }

    char *ct = NULL;
    res = curl_easy_getinfo(curl_handle, CURLINFO_CONTENT_TYPE, &ct);
    if ( res != CURLE_OK || ct == NULL ) {
        fprintf(stderr, "Failed to obtain Content-Type\n");
        return 2;
    }

    if (strstr(ct, CT_HTML) ) {
        process_html(curl_handle, p_recv_buf, url);
    } else if (strstr(ct, CT_PNG)) {
        process_png(curl_handle, p_recv_buf, url);
    } else {
        add_visited(url);
    }
    return 0;
}

void init_url(url_node *url, char *data) {
    url->next = NULL;
    if (data != NULL) {
        strcpy(url->url, data);
    } else {
        url -> url[0] = '\0';
    }
}

void web_crawl() {
    int still_running = 0;
    int numfds = 0;
    int res;
    CURLMsg *msg = NULL;
    int msgs_left = 0;
    CURL *eh;
    CURLcode return_code;

    curl_multi_perform(cm, &still_running);
    while(png_num > 0) {
        /* wait for data */
        numfds = 0;
        do {
            res = curl_multi_wait(cm, NULL, 0, 0, &numfds);
            if (res != CURLM_OK){
                printf("error: curl_multi_wait() returned %d\n", res);
                return;
            }
            curl_multi_perform(cm, &still_running);
            if (still_running == 0){
                break;
            }
        } while (numfds == 0); 
        /* process resulting messages */
        while ((msg = curl_multi_info_read(cm, &msgs_left)) && png_num > 0){
            if (msg->msg == CURLMSG_DONE){
                eh = msg->easy_handle;
                return_code = msg->data.result;
                int index = find_connection(eh);
                printf("processing index %d\n", index);
            	if (index == -1) continue;
                if (return_code == CURLE_OK && curl_handles[index].buf != NULL && curl_handles[index].buf->size > 0) {
					process_data(eh, curl_handles[index].buf, curl_handles[index].url);
                } else {
                    /* broken link */
					add_visited(curl_handles[index].url);
                }
                /* remove handle */
                recv_buf_cleanup(curl_handles[index].buf);
                curl_handles[index].url[0] = '\0';
                curl_multi_remove_handle(cm, eh);
                curl_easy_cleanup(curl_handles[index].curl);
            }
        }
		if (still_running == 0 && frontier_list == NULL) break;
		else get_url();
	}
}

void get_url() {
    RECV_BUF *recv_buf;
    for (int i = 0; i < conc_num; i++) {
	    if (curl_handles[i].url[0] == '\0') {
		    char *frontier_url = strdup(frontier_list->url);
		    pop_frontiers();
		    recv_buf = malloc(sizeof(RECV_BUF));
		    recv_buf_init(recv_buf, BUF_SIZE);
		    curl_handles[i].curl = curl_easy_init();
		    curl_handles[i].buf = recv_buf;
		    strcpy(curl_handles[i].url, frontier_url);
		    printf("popped url: %s\n", curl_handles[i].url);
		    easy_handle_init(curl_handles[i].curl, curl_handles[i].buf,curl_handles[i].url); 
		    free(frontier_url);
	    }
	    if (frontier_list == NULL) break;
    }
}

void write_png(){
    /* write png urls to file */
    FILE *fp = fopen("png_urls.txt", "w");
    url_node *cur = png_list;
    while(cur != NULL){
        fwrite(cur->url, 1, strlen(cur->url), fp);
        fprintf(fp, "\n");
        cur = cur->next;        
    }
    fclose(fp);
}

void write_log(char* file_name, char *url) {
    /* write visited urls to log file */
    FILE *fp = fopen(file_name, "a");
    fwrite(url, 1, strlen(url), fp);
    fprintf(fp, "\n");
    fclose(fp);
}

/* returns 1 if the file is a PNG */
int is_png(RECV_BUF *data) {
    int png_file = 0;
    char* file_signature = malloc(8*sizeof(char));
    char* png_id = "\x89\x50\x4E\x47\x0D\x0A\x1A\x0A";
    memcpy(file_signature, data->buf, 8);
    if (memcmp(png_id, file_signature,8) == 0) {
		png_file = 1;
    }
    free(file_signature);
    return png_file;
}

/* return index of connection with given url */
int find_connection(CURL *eh) {
    for (int i = 0; i < conc_num; i++){
        if (curl_handles[i].url[0] != '\0' && curl_handles[i].curl == eh) {
			return i;
		}
    }
    return -1;
}
