from threading import Thread
import threading
import socket
import sys
import os
import time
import urllib.request
import json

# import uvicorn

SHARED_DIR = "shared"
INPUT_DIR = "input"
OUTPUT_FILE = "output"
NODE_PREFIX = "node_"

class CounterApp(Thread):
    def __init__(self, bully_algorithm):
        Thread.__init__(self)
        self.quit = False
        self._my_ip = None
        self.other_nodes = []
        self.discovery_ready = False
        self.bully_algorithm = bully_algorithm
        self.task_lock = threading.Lock()

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
                if not node_ip in self.other_nodes and node_ip != self.get_my_ip():
                    self.other_nodes.append(node_ip)

            # Exit discovery loop after window closes
            if time.time() - window_start >= (init_window_ms/1000.0):
                break

            # Wait 1/10 of the wait windows at a time
            time.sleep(init_window_ms/10000.0)

        print("Purge node table of {}!".format(self.get_my_ip()))

        # Next, we purge discovery files and node entries that do not answer
        working_other_nodes = []
        for node_ip in self.other_nodes:
            url = "http://{}:8000/status".format(node_ip)
            try: 
                print("Discovery from {} to {} ...".format(self.get_my_ip(), url))
                contents = urllib.request.urlopen(url, timeout=0.2).read()
                print("Discovery from {} to {} returned {}".format(self.get_my_ip(), url, contents))
                working_other_nodes.append(node_ip)
            except:
                #This is expected to fail with multiple other_nodes as the file might already be removed.
                print("Discovery from {} to {} FAILED!".format(self.get_my_ip(), url))
                try:
                    os.remove(self.get_discovery_file_name(node_ip))
                    print("Node {} purges discovery file of {}".format(self.get_my_ip(), node_ip))
                except:
                    pass
        self.other_nodes = working_other_nodes
        self.discovery_ready = True

        print("Discovery ready for {}, other_nodes: {}!".format(self.get_my_ip(), self.other_nodes))

    def run_leader(self, worker_timeout_ms=10000):
        print("Running {} as leader".format(self.get_my_ip()))

        input_path = SHARED_DIR + os.sep + INPUT_DIR
        if not os.path.exists(input_path):
            print(f"No directory '{INPUT_DIR}', exiting...")
            self._kill_workers();
            sys.exit(-1)

        files = []
        for entry in os.listdir(input_path):
            files.append(input_path + os.sep + entry)

        free_workers = self._all_nodes()

        total_n_files = len(files)
        file_results = []

        while True:
            self.task_lock.acquire()
            if len(file_results) == total_n_files:
                self.task_lock.release()
                break

            if len(free_workers) == 0:
                self.task_lock.release()
                time.sleep(0.05)
                continue

            self.task_lock.release()

            t = threading.Thread(target=self._assign_task,
                args=(files, free_workers, file_results,
                      worker_timeout_ms))
            t.start()

        total_result = {}

        print("Merging partial results...")
        for result in file_results:
            print(result)
            for word in result:
                total_result[word] = total_result.get(word, 0) + result[word]

        with open(SHARED_DIR + os.sep + OUTPUT_FILE, 'w') as f:
            json.dump(total_result, f)

        print("Word counting result:", total_result)
        self._kill_workers()


    def _assign_task(self, files, free_workers, file_results,
            worker_timeout_ms):
        self.task_lock.acquire()
        if len(files) == 0 or len(free_workers) == 0:
            self.task_lock.release()
            return

        file = files.pop()
        worker = free_workers.pop()

        self.task_lock.release()

        url = f"http://{worker}:8000/assign"
        data = urllib.parse.urlencode({"file": file})
        data = data.encode('ascii')

        try:
            q = urllib.request.urlopen(url, data, timeout=worker_timeout_ms)
            result = json.load(q)
        except Exception as ex:
            print(f"Worker {worker} failed returning task result for {file}")
            # Put back the unprocessed file but do not put back the
            # failed worker
            self.task_lock.acquire()
            files.append(file)
            self.task_lock.release()
            return

        self.task_lock.acquire()
        free_workers.append(worker)
        file_results.append(result)
        self.task_lock.release()


    def run(self):
        print("CounterApp {} starting...".format(self.get_my_ip()))

        self.discovery(3000)

        self.bully_algorithm.hold_election(self.get_my_ip(), self.other_nodes)
        print("Node {}'s leader: {}".format(self.get_my_ip(), self.bully_algorithm.leader_ip))

        print("CounterApp {} running...".format(self.get_my_ip()))

        if self.get_my_ip() == self.bully_algorithm.leader_ip:
            self.run_leader()


    def _kill_workers(self):
        for node in self._all_nodes():
            url = f"http://{node}:8000/die"
            try:
                urllib.request.urlopen(url)
            except:
                pass

    def _all_nodes(self):
        return self.other_nodes + [self.get_my_ip()]
