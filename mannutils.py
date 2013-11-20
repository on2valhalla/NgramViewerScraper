import sys
import datetime


# A function that prints to a writeable buffer with a human readable timestamp and an optional begin and end strings
# such as \r (return: for rewriting the same line) or \n
def print_time(string, begin='', end='\n'):
    sys.stderr.write("%s[%s]\t%s%s" % (begin, str(datetime.datetime.now().time())[:8], string, end))
    sys.stderr.flush()