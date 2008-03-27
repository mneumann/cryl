/*
 * Hyper-fast link extractor.
 *
 * Extracts links from HTML files (everything between HREF="...").
 *
 * Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
 */

#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/uio.h>
#include "parse.h"

#define LINE_BUF_SIZE (4*1024)
#define LINKS_EXT ".links"
#define LINKS_EXT_SIZE 6 
#define DATA_EXT ".data"
#define DATA_EXT_SIZE 5
#define HREF_BUF_SIZE (4*1024)

static char line_buf[LINE_BUF_SIZE];
static char href_buf[HREF_BUF_SIZE];

/*
 * Chops string +str+ at character +x+.
 */
void chop(char *str, char x)
{
  char *p; if ((p = index(str, x))) *p = '\0';
}

int
main(int argc, char **argv)
{
  int fh, of, cont;
  void *p;
  off_t len;
  struct parse_state s;

  while (fgets(line_buf, LINE_BUF_SIZE-LINKS_EXT_SIZE, stdin))
  {
    chop(line_buf, '\n');
    chop(line_buf, '\r');

    fh = open(line_buf, O_RDONLY);
    if (fh == -1)
    {
      fprintf(stderr, "OPEN: %s\n", line_buf);
      continue;
    }

    len = lseek(fh, 0, SEEK_END); 
    if (len == -1)
    {
      fprintf(stderr, "LSEEK: %s\n", line_buf);
      close(fh);
      continue;
    }
    if (lseek(fh, 0, SEEK_SET) != 0)
    {
      fprintf(stderr, "LSEEK2: %s\n", line_buf);
      close(fh);
      continue;
    }

    p = mmap(NULL, len, PROT_READ, MAP_PRIVATE, fh, 0);
    if (p == MAP_FAILED)
    {
      fprintf(stderr, "MMAP: %s\n", line_buf);
      close(fh);
      continue;
    }

    /*
     * Open output file.
     *
     * Output file is filename read on stdin with ".ext" stripped
     * off (if it has an extension) + ".links".
     */
    
    char *ext = rindex(line_buf, '.');

    if (ext != NULL && ext >= rindex(line_buf, '/'))
    {
      // strip off extension
      *ext = '\0';
    }
    strcat(line_buf, LINKS_EXT);

    of = open(line_buf, O_WRONLY|O_CREAT, 0444);
    if (of == -1)
    {
      fprintf(stderr, "LINKS-OPEN: %s\n", line_buf);
      munmap(p, len);
      close(fh);
      continue;
    }

    parse_init(&s, (char*)p, (char*)p+len, href_buf, HREF_BUF_SIZE-1);
    while (1)
    {
      cont = parse(&s);
      if (s.copy_buf_pos > 0)
      {
        s.copy_buf[s.copy_buf_pos++] = '\n';
        write(of, s.copy_buf, s.copy_buf_pos);
        s.copy_buf_pos = 0;
      }
      if (!cont) break;
    }

    munmap(p, len);
    close(of);
    close(fh);
  }
  return 0;
}
