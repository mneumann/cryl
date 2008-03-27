struct parse_state {
  int state;
  char *p;
  char *last;

  char *copy_buf;
  int copy_buf_sz;
  int copy_buf_pos;
};

void parse_init(struct parse_state *s, char *p, char *last, char *copy_buf, int copy_buf_sz);
int parse(struct parse_state *s);
