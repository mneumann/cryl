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

/*
 * Chops string +str+ at character +x+.
 */
void chop(char *str, char x)
{
  char *p; if ((p = index(str, x))) *p = '\0';
}

#define LINE_BUF_SIZE (4*1024)
#define LINKS_EXT ".links"
#define LINKS_EXT_SIZE 6 

static char line_buf[LINE_BUF_SIZE];
static char line_buf2[LINE_BUF_SIZE+LINKS_EXT_SIZE];

#define HREF_BUF_SIZE (4*1024)
static char href_buf[HREF_BUF_SIZE];

void
output_href(int i, int of)
{
  if (i <= 0) return;
  href_buf[i++] = '\n';
  write(of, href_buf, i);
}

void
parse(char *p, char *last, int of)
{
  char ch, quot;
  int i;

  enum {
    sw_start = 0,
    sw_H,
    sw_HR,
    sw_HRE,
    sw_HREF,
    sw_HREF_eq,
    sw_HREF_quot,
    sw_HREF_content
  } state;

  i = 0;
  state = sw_start;

  for (; p < last; p++) 
  {
    ch = *p;

    switch (state) {

      case sw_start:

        if (ch == 'h' || ch == 'H')
          state = sw_H; 
        break;

      case sw_H:

        if (ch == 'r' || ch == 'R')
          state = sw_HR; 
        else
          state = sw_start;
        break;

      case sw_HR:

        if (ch == 'e' || ch == 'E')
          state = sw_HRE; 
        else
          state = sw_start;
        break;

      case sw_HRE:

        if (ch == 'f' || ch == 'F')
          state = sw_HREF; 
        else
          state = sw_start;
        break;

      case sw_HREF:

        /* skip ws */
        if (isspace(ch))
          break;

        if (ch == '=')
          state = sw_HREF_eq; 
        else
          state = sw_start;
        break;

      case sw_HREF_eq:

        /* skip ws */
        if (isspace(ch))
          break;

        if (ch == '"' || ch == '\'')
        {
          state = sw_HREF_quot; 
          quot = ch;
        }
        else
          state = sw_start;
        break;

      case sw_HREF_quot:

        /* skip ws */
        if (isspace(ch))
          break;

        if (ch == quot)
        {
          /* empty href */
          state = sw_start;
          break;
        }

        i = 0;
        state = sw_HREF_content; 
        /* fallthrough! */

      case sw_HREF_content:

        if (ch == quot || i >= HREF_BUF_SIZE-2)
        {
          output_href(i, of);

          state = sw_start;
          break;
        }

        if (isspace(ch))
        {
          /* remove newlines */
          ch = ' '; 
        }

        href_buf[i++] = ch;
        break;

    }
  }

  output_href(i, of);
}


int
main(int argc, char **argv)
{
  int fh, of;
  void *p;
  off_t len;

  while (fgets(line_buf, LINE_BUF_SIZE, stdin))
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

    p = mmap(NULL, len, PROT_READ, 0, fh, 0);
    if (p == MAP_FAILED)
    {
      fprintf(stderr, "MMAP: %s\n", line_buf);
      close(fh);
      continue;
    }

    /*
     * Open output file
     */
    line_buf2[0] = '\0';
    strcat(line_buf2, line_buf);
    strcat(line_buf2, LINKS_EXT);

    of = open(line_buf2, O_WRONLY|O_CREAT, 0444);
    if (of == -1)
    {
      fprintf(stderr, "LINKS-OPEN: %s\n", line_buf2);
      munmap(p, len);
      close(fh);
      continue;
    }

    parse((char*)p, (char*)p+len, of);

    munmap(p, len);
    close(of);
    close(fh);
  }
  return 0;
}
