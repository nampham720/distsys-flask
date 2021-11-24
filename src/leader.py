import time
import urllib
from concurrent.futures import ThreadPoolExecutor

class LeaderManager:
    def __init__(self, election_window, message_timeout):
        '''Constructs LeaderManager.

        @param election_window:  The maximum length of the election in seconds
        @param message_timeout:
            Timeout for node-to-node communication in seconds. If no reseponse
            is received within the timeout, the target node is considered to
            be down.
        '''
        self.leader_ip = None
        self._election_window = election_window
        self._message_timeout = message_timeout
        self._last_election = 0

    def _in_election(self):
        return self._last_election + self._election_window > time.time()

    def _mark_started(self):
        self._last_election = time.time()

    def _broadcast_election(self, own_ip, other_nodes):
        larger_nodes = list(filter(lambda x: x > own_ip, other_nodes))

        def node_answers(target_node):
            url = f"http://{target_node}:8000/election"
            req = urllib.request.Request(url, method="POST")
            try:
                urllib.request.urlopen(req, timeout=self._message_timeout)
                return True
            except Exception as ex:
                print(f"Node {own_ip} failed broadcasting election to {target_node}: {ex}")
                return False

        with ThreadPoolExecutor(max(1, len(larger_nodes))) as executor:
            is_leader = True not in executor.map(node_answers, larger_nodes)
            return is_leader

    def _broadcast_victory(self, own_ip, other_nodes):
        self.leader_ip = own_ip

        def f(target_node):
            url = f"http://{target_node}:8000/victory"
            data = urllib.parse.urlencode({"leader_ip": own_ip})
            data = data.encode('ascii')
            try:
                urllib.request.urlopen(
                    url, data, timeout=self._message_timeout)
            except Exception as ex:
                print(f"Node {own_ip} failed broadcasting victory to {target_node}: {ex}")

        with ThreadPoolExecutor(max(1, len(other_nodes))) as executor:
            executor.map(f, other_nodes)

    def _hold_election(self, own_ip, other_nodes):
        if self._in_election():
            return
        self._mark_started()
        if self._broadcast_election(own_ip, other_nodes):
            self._broadcast_victory(own_ip, other_nodes)

    def hold_election(self, own_ip, other_nodes, wait=True):
        '''Holds leader election.

        @param own_ip: Node's ip address
        @param other_nodes: List of other node's ip addresses
        @param wait: Whether the function should wait for finishing the election
        '''
        self._hold_election(own_ip, other_nodes)
        if wait:
            time.sleep(self._election_window)
