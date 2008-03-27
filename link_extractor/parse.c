#include "parse.h"

void
parse_init(struct parse_state *s, char *p, char *last, char *copy_buf, int copy_buf_sz)
{
  s->state = 0;
  s->p = p;
  s->last = last;
  s->copy_buf = copy_buf;
  s->copy_buf_sz = copy_buf_sz;
  s->copy_buf_pos = 0;
}

/*
 * +copy_buf_sz+ is the number of bytes that can be copied into.
 *
 * Returns true or false, depending on whether the end was reached
 * or not. 
 */
int
parse(struct parse_state *s)
{
  char ch, quot;

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

  state = s->state; 

  for (; s->p < s->last; s->p++) 
  {
    ch = *(s->p);

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

        s->copy_buf_pos = 0;
        state = sw_HREF_content; 
        /* fallthrough! */

      case sw_HREF_content:

        if (ch == quot || s->copy_buf_pos >= s->copy_buf_sz)
        {
          /*
           * It's probably a missing ending quotation.
           */ 

          s->state = sw_start;

          /* must reset copy_buf_pos?! */
          return (0==0); 
        }

        if (isspace(ch))
        {
          /* remove newlines */
          ch = ' '; 
        }

        s->copy_buf[s->copy_buf_pos++] = ch;
        break;

    }
  }

  s->state = state;
  return (0==1); /* end reached */
}
