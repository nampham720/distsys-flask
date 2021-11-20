from threading import Thread
import socket
import sys
import os
import time
import urllib.request

# import uvicorn

SHARED_DIR = "shared"
NODE_PREFIX = "node_"

class CounterApp(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.quit = False
        self._my_ip = None
        self.nodes = []

    def get_my_ip(self):
        if self._my_ip:
            return self._my_ip
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        self._my_ip = s.getsockname()[0]
        s.close()
        return self._my_ip

    def get_discovery_file_name(self, ip_addr):
        return SHARED_DIR+os.sep+NODE_PREFIX+ip_addr

    def discovery(self, init_window_ms=0):

        if not os.path.exists(SHARED_DIR):
            print("No directory 'shared', exiting...")
            sys.exit(-1)

        try:
            file = open(self.get_discovery_file_name(self.get_my_ip()), "w")
            file.write(self.get_my_ip())
            file.close()
            print("Written discovery file for {} to {}!".format(self.get_my_ip(), os.path.abspath(self.get_discovery_file_name(self.get_my_ip()))))
        except:
             print("Can't write discovery file for {}, tried to {}!".format(self.get_my_ip(), os.path.abspath(self.get_discovery_file_name(self.get_my_ip()))))
        
        window_start = time.time()
        while True: 
            # List directory contents 
            for entry in os.listdir(SHARED_DIR):

                # Skip non-discovery files
                if not entry.startswith(NODE_PREFIX):
                    continue

                node_ip = entry[len(NODE_PREFIX):]
                if not node_ip in self.nodes and node_ip != self.get_my_ip():
                    self.nodes.append(node_ip)

            # Exit discovery loop after window closes
            if time.time() - window_start >= (init_window_ms/1000.0):
                break

            # Wait 1/10 of the wait windows at a time
            time.sleep(init_window_ms/10000.0)

        print("Purge node table of {}!".format(self.get_my_ip()))

        # Next, we purge discovery files and node entries that do not answer
        working_nodes = []
        for node_ip in self.nodes:
            url = "http://{}:8000/status".format(node_ip)
            try: 
                print("Discovery from {} to {} ...".format(self.get_my_ip(), url))
                contents = urllib.request.urlopen(url, timeout=0.2).read()
                print("Discovery from {} to {} returned {}".format(self.get_my_ip(), url, contents))
                working_nodes.append(node_ip)
            except:
                #This is expected to fail with multiple nodes as the file might already be removed.
                print("Discovery from {} to {} FAILED!".format(self.get_my_ip(), url))
                try:
                    os.remove(self.get_discovery_file_name(node_ip))
                    print("Node {} purges discovery file of {}".format(self.get_my_ip(), node_ip))
                except:
                    pass
        self.nodes = working_nodes

        print("Discovery ready for {}, nodes: {}!".format(self.get_my_ip(), self.nodes))

    def run(self):
        print("CounterApp {} starting...".format(self.get_my_ip()))

        self.discovery(3000)

        while not self.quit:
            time.sleep(1)
            print("CounterApp {} running...".format(self.get_my_ip()))

        print("CounterApp {} exiting...".format(self.get_my_ip()))
