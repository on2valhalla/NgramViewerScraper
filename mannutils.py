import sys
import datetime
from os import walk
from os.path import isfile, join, basename
from shutil import move


# A function that prints to a writeable buffer with a human readable timestamp and an optional begin and end strings
# such as \r (return: for rewriting the same line) or \n
def print_time(string, begin='', end='\n'):
    sys.stderr.write("%s[%s]  %s%s" % (begin, str(datetime.datetime.now().time())[:8], string, end))
    sys.stderr.flush()


# Helper function to return a list of files in a directory that match a pre-compiled regex expressions.
def regex_find_files(compiled_re, rootdir, recursive=False):
    if compiled_re is None or rootdir is None:
        return []
    matched_files = []
    #print rootdir
    for (dirpath, dirnames, filenames) in walk(rootdir):
        #print '\t', dirpath
        for filename in filenames:
            #print '\t\t', filename
            if compiled_re.match(filename) and isfile(join(rootdir, dirpath, filename)):
                matched_files.append(join(rootdir, dirpath, filename))
        if not recursive:
            break
    return matched_files


def move_proc_safe(move_queue, output_queue, prev_events, this_event, dest_dir, num_files, max_len=50):
    digits = len(str(num_files))
    i = 0
    while any(not e.is_set() for e in prev_events) or not move_queue.empty():
        i += 1
        full_tmp_path = move_queue.get()
        base = basename(full_tmp_path)
        full_dest_path = join(dest_dir, base)
        #output_queue.put('(%-*i of %i) Moving:\t%*s to %*s'
        output_queue.put('(%-*i of %i) Moving:\t%*s'
                          % (digits, i, num_files, max_len, base))
        move(full_tmp_path, full_dest_path)

    this_event.set()