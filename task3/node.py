import argparse
from concurrent import futures
import threading
import random
import time

import grpc

import raft_pb2 as pb2
import raft_pb2_grpc as pb2_grpc


# Enum class with node states
class NodeState:
    LEADER = 'Leader'
    CANDIDATE = 'Candidate'
    FOLLOWER = 'Follower'


NODE_ID = None
SERVERS_INFO = {}
SUSPEND = None
LEADER_ID = None
STATE = NodeState.FOLLOWER
TERM = 0
VOTED = False
UNCOMMITED_VALUE = 0
COMMITED_VALUE = 0
ELECTION_TIMEOUT = 0 # Initialized after random
APPEND_ENTRIES_TIMEOUT = 0.4


def print_state():
    print('STATE: ' + STATE + ' | TERM: ' + str(TERM))

def send_heartbeat():
    global heartbeat_timer, COMMITED_VALUE
    n_received = 0
    for server_id, server_addr in SERVERS_INFO.items():
        # Connect and send request
        channel = grpc.insecure_channel(server_addr)
        server = pb2_grpc.RaftNodeStub(channel)
        try:
            result = server.AppendEntries(
                pb2.AppendEntriesArgs(leader_id=NODE_ID, leader_term=TERM, committed_value=COMMITED_VALUE, uncommitted_value=UNCOMMITED_VALUE)
            )
            if result.heartbeat_result:
                n_received += 1
        except grpc._channel._InactiveRpcError:
            pass
    if n_received > len(SERVERS_INFO) / 2:
        COMMITED_VALUE = UNCOMMITED_VALUE
    heartbeat_timer = threading.Timer(APPEND_ENTRIES_TIMEOUT, send_heartbeat)
    heartbeat_timer.start()
    

def initialize_election():
    global VOTED, TERM, election_timer, STATE, LEADER_ID, heartbeat_timer
    print('TIMEOUT Expired | Leader Died')
    print_state()
    election_timer.cancel()
    STATE = NodeState.CANDIDATE
    TERM += 1

    voted_to_me = 0

    for server_id, server_addr in SERVERS_INFO.items():
        # Connect and send request
        channel = grpc.insecure_channel(server_addr)
        server = pb2_grpc.RaftNodeStub(channel)
        try:
            result = server.RequestVote(
                pb2.RequestVoteArgs(candidate_id=NODE_ID, candidate_term=TERM)
            )
        except grpc._channel._InactiveRpcError:
            pass
        
        if result.vote_result:
            voted_to_me += 1
    print('Votes aggregated')

    VOTED = False
    if voted_to_me > len(SERVERS_INFO) / 2:
        STATE = NodeState.LEADER
        LEADER_ID = NODE_ID
        send_heartbeat()
    else:
        STATE = NodeState.FOLLOWER
        election_timer = threading.Timer(ELECTION_TIMEOUT, initialize_election)
        election_timer.start()
    print_state()

election_timer = None
heartbeat_timer = None


class Handler(pb2_grpc.RaftNodeServicer):
    def __init__(self):
        global election_timer, ELECTION_TIMEOUT
        super().__init__()

        # Some initialization was moved, but it was in "DO NOT CHANGE" section
        random.seed(time.time() * NODE_ID)
        ELECTION_TIMEOUT = random.uniform(2, 4)

        election_timer = threading.Timer(ELECTION_TIMEOUT, initialize_election)
        election_timer.start()

    def AppendEntries(self, request, context):
        global election_timer, LEADER_ID, COMMITED_VALUE, UNCOMMITED_VALUE
        if SUSPEND:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.AppendEntriesResponse()

        if request.leader_term < TERM:
            print('TERM is too old')
            return pb2.AppendEntriesResponse(**{"term": TERM, "heartbeat_result": False})

        if STATE == NodeState.FOLLOWER:
            LEADER_ID = request.leader_id
            COMMITED_VALUE = request.committed_value
            UNCOMMITED_VALUE = request.uncommitted_value
            election_timer.cancel()
            election_timer = threading.Timer(ELECTION_TIMEOUT, initialize_election)
            election_timer.start()
            VOTED = False

        return pb2.AppendEntriesResponse(**{"term": TERM, "heartbeat_result": True})


    def RequestVote(self, request, context):
        global VOTED, TERM, election_timer
        print_state()
        if SUSPEND:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.RequestVoteResponse()
        print(f'RPC[RequestVote] Invoked')
        print(f'\tArgs:')
        print(f'\t\tcandidate_id: {request.candidate_id}')
        print(f'\t\tcandidate_term: {request.candidate_term}')


        if STATE != NodeState.LEADER and not VOTED and \
            (request.candidate_term > TERM or request.candidate_id == NODE_ID):
            TERM = request.candidate_term
            VOTED = True

            print('Voted for NODE ' + str(request.candidate_id))
            return pb2.RequestVoteResponse(**{"term": TERM, "vote_result": True})

        return pb2.RequestVoteResponse(**{"term": TERM, "vote_result": False})

    def GetLeader(self, request, context):
        print_state()
        if SUSPEND:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.GetLeaderResponse()
        print(f'RPC[GetLeader] Invoked')
        return pb2.GetLeaderResponse(**{"leader_id": LEADER_ID})
        
    def AddValue(self, request, context):
        global UNCOMMITED_VALUE
        print_state()
        if SUSPEND:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.AddValueResponse()
        print(f'RPC[AddValue] Invoked')
        print(f'\tArgs:')
        print(f'\t\tvalue_to_add: {request.value_to_add}')

        if STATE == NodeState.LEADER:
            UNCOMMITED_VALUE += request.value_to_add
        else:
            server_addr = SERVERS_INFO[LEADER_ID]
            channel = grpc.insecure_channel(server_addr)
            server = pb2_grpc.RaftNodeStub(channel)
            server.AddValue(request)

        return pb2.AddValueResponse(**{})
    
    def GetValue(self, request, context):
        print_state()
        if SUSPEND:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.GetValueResponse()
        print(f'RPC[GetValue] Invoked')
        return pb2.GetValueResponse(**{"value": COMMITED_VALUE})

    def Suspend(self, request, context):
        global election_timer, STATE, SUSPEND
        print_state()
        print(f'RPC[Suspend] Invoked')
        SUSPEND = True
        election_timer.cancel()
        if heartbeat_timer:
            heartbeat_timer.cancel()
        return pb2.SuspendResponse(**{})
    
    def Resume(self, request, context):
        global election_timer, STATE, SUSPEND
        print_state()
        print(f'RPC[Resume] Invoked')
        SUSPEND = False
        STATE = NodeState.FOLLOWER
        election_timer.cancel()
        election_timer = threading.Timer(ELECTION_TIMEOUT, initialize_election)
        election_timer.start()
        return pb2.ResumeResponse(**{})


# ----------------------------- Do not change ----------------------------- 
def serve():
    print(f'NODE {NODE_ID} | {SERVERS_INFO[NODE_ID]}')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_RaftNodeServicer_to_server(Handler(), server)
    server.add_insecure_port(SERVERS_INFO[NODE_ID])

    try:
        server.start()
        while True:
            server.wait_for_termination()
    except grpc.RpcError as e:
        print(f"Unexpected Error: {e}")
    except KeyboardInterrupt:
        server.stop(grace=10)
        print("Shutting Down...")


def init(node_id):
    global NODE_ID
    NODE_ID = node_id

    with open("config.conf") as f:
        global SERVERS_INFO
        lines = f.readlines()
        for line in lines:
            parts = line.split()
            id, address = parts[0], parts[1]
            SERVERS_INFO[int(id)] = str(address)
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("node_id", type=int)
    args = parser.parse_args()

    init(args.node_id)

    serve()
# ----------------------------- Do not change -----------------------------
