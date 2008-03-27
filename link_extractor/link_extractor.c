#include "ruby.h"
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/uio.h>
#include "parse.h"

#define HREF_BUF_SIZE (4*1024)
static char href_buf[HREF_BUF_SIZE];

VALUE
LinkExtractor__parse(VALUE self, VALUE filename)
{
  int fh, cont;
  void *p;
  off_t len;
  struct parse_state s;
  VALUE str;

  fh = open(RSTRING_PTR(filename), O_RDONLY);
  if (fh == -1)
  {
    return Qfalse;
  }

  len = lseek(fh, 0, SEEK_END); 
  if (len == -1)
  {
    close(fh);
    return Qfalse;
  }
  if (lseek(fh, 0, SEEK_SET) != 0)
  {
    close(fh);
    return Qfalse;
  }

  p = mmap(NULL, len, PROT_READ, MAP_PRIVATE, fh, 0);
  if (p == MAP_FAILED)
  {
    close(fh);
    return Qfalse;
  }

  parse_init(&s, (char*)p, (char*)p+len, href_buf, HREF_BUF_SIZE-2);

  while (1)
  {
    cont = parse(&s);
    if (s.copy_buf_pos > 0)
    {
      str = rb_str_new(s.copy_buf, s.copy_buf_pos);
      rb_yield(str);
      s.copy_buf_pos = 0;
    }
    if (!cont) break;
  }

  munmap(p, len);
  close(fh);

  return Qtrue;
}

void Init_link_extractor()
{
  rb_define_method(rb_define_class("LinkExtractor", rb_cObject), "parse", LinkExtractor__parse, 1);
}
