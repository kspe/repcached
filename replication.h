#ifndef MEMCACHED_REPLICATION_H
#define MEMCACHED_REPLICATION_H
#define REPCACHED_VERSION "2.3.1"
#include <netdb.h>

enum CMD_TYPE {
  REPLICATION_REP,
  REPLICATION_DEL,
  REPLICATION_FLUSH_ALL,
  REPLICATION_DEFER_FLUSH_ALL,
  REPLICATION_MARUGOTO_END,
};

typedef struct queue_item_t Q_ITEM;
struct queue_item_t {
  enum CMD_TYPE  type;
  char          *key;
  rel_time_t     time;
  Q_ITEM        *next;
};

typedef struct replication_cmd_t R_CMD;
struct replication_cmd_t {
  char       *key;
  int         keylen;
  rel_time_t  time;
};

Q_ITEM *qi_new(enum CMD_TYPE type, R_CMD *cmd, bool);
void    qi_free(Q_ITEM *);
int     qi_free_list(void);
int     replication_cmd(conn *, Q_ITEM *);
int     get_qi_count(void);

void    replication_queue_push(Q_ITEM *);
Q_ITEM *replication_queue_pop(void);

int replication_call_rep(char *key, size_t keylen);
int replication_call_del(char *key, size_t keylen);
int replication_call_flush_all(void);
int replication_call_defer_flush_all(const rel_time_t time);
int replication_call_marugoto_end(void);
int replication(enum CMD_TYPE type, R_CMD *cmd);

#endif
