
from mechanize import Browser
from os import remove, rename, stat
from os.path import join, basename, exists
from time import time
from urllib2 import URLError


class Downloader(object):
    """
    Downloads files synchronously based on regular expressions,
    but pushes finished files up to caller in a multiprocessing.Queue.
    """
    def __init__(self, download_q, parse_q, output_q, download_event):
        self.download_q = download_q
        self.parse_q = parse_q
        self.output_q = output_q
        self.download_event = download_event

    def regex_find_links(self, page, regex):
        br = Browser()
        br.set_handle_robots(False)
        try:
            br.open(page)
        except URLError:
            self.output_q.put('URL Not Found: %s' % page)

        # Number of matching links found
        num_links = 0
        # Max length of printed links
        max_len = 0
        #print page, regex
        for link in br.links():
            if regex.match(link.url):
                num_links += 1
                max_len = max(len(basename(link.url)), max_len)
                # Put the found links on the download queue
                self.download_q.put(link.url)
                # Put the output messages on the respective queue.
                # Implemented this way just in case this method is also threaded/multi-processed.
                # NOTE: Try to keep the number of output messages the same as the links
                self.output_q.put('Found Link: %s' % link.url)

        return num_links, max_len

    # Downloads links from the download queue to files and outputs progress.
    # NOTE: It is probably better if for each link something gets put on the parse queue, for now: None.
    def download(self, download_dir, num_links, max_len=50, dest_dir=None):
        # Setup browser
        br = Browser()
        br.set_handle_robots(False)

        # String formatting setup
        digits = len('%d' % num_links)
        start_time = time()

        # Iterate over all the links
        i = 0
        num_files = 0
        full_path = None
        while not self.download_q.empty():
            # Attempt to download the file
            download_start = time()
            i += 1
            try:
                link = self.download_q.get()
                file_name = basename(link)
                full_path = join(download_dir, file_name)

                # Check if the file already exists
                if exists(full_path) or (dest_dir and exists(join(dest_dir, file_name))):
                    self.output_q.put('(%-*i of %i) File exists:\t%*s'
                                      % (digits, i, num_links, max_len, file_name))
                    continuet

                self.output_q.put('(%-*i of %i) Downloading:\t%*s'
                                  % (digits, i, num_links, max_len, file_name))
                br.retrieve(link, filename=full_path)
                br.clear_history()
                num_files += 1
            except KeyboardInterrupt:
                self.output_q.put('(%-*i of %i) User Interrupt:\t%*s'
                                  % (digits, i, num_links, max_len, "Deleting: %s" % full_path))
                if exists(full_path):
                    remove(full_path)
                i -= 1
                break
            except URLError:
                self.output_q.put('(%-*i of %i) URL Error:\t%*s'
                                  % (digits, i, num_links, max_len, "Skipping: %s" % full_path))
                remove(full_path)
            self.parse_q.put(full_path)
            self.output_q.put('(%-*i of %i) Downloaded:\t%*s' % (digits, i, num_links, max_len,
                                                                 '%s MB in %s min' % ("{:7.2f}".format(float(
                                                                   stat(full_path).st_size)/1000000),
                                                                 "{:5.2f}".format((time() - download_start)/60))))

        self.output_q.put('Downloaded %i files to %s in %5.2f min' % (i, download_dir,
                                                                     (time() - start_time)/60))
        self.download_event.set()

