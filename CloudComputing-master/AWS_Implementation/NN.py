from flask import Flask, make_response
from flask_restful import Api, Resource, reqparse, request
import requests
import simplejson as json
import datetime
import threading
import atexit


app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()                                       # JSON parser
parser.add_argument('file')

# NN data
master_DNlists_dict = {}                                                # master list of all DN lists
master_heartbeat_dict = {}                                              # master list for block reports / heart beats

# NN Setup
block_size = 64000000                                                   # 64 Megabyte block size
replication_factor = 3
err_code = 400
port = 5000
err_message = "ERROR"
fault_tolerance = "/FT"                                                 # listen for POSTs from NN

# Threading variables
DataLock = threading.Lock()                                             # Lock to control access to data
yourThread = threading.Thread()                                         # Thread handler
wait_time = 45                                                          # Time between block beats


class NN_server(Resource):

    def get(self):

        parser = reqparse.RequestParser()
        parser.add_argument("filename")                                 # name of key
        args = parser.parse_args()
        filename = args["filename"]                                     # payload from client containing block id

        print("\nClient requested file: ", filename, " - checking if I have it...", end="")

        # if I have the file, send the DN list back
        if filename in master_DNlists_dict.keys():
            print("I HAVE file:", filename, "\n")
            dn_list = master_DNlists_dict[filename]
            return dn_list

        # else, return ERROR
        else:
            print("I do NOT have file:", filename, "\n")
            return err_message

    def post(self):

        # these are the files that NN currently has
        files_list = master_DNlists_dict.keys()

        # get file name and size from client
        cli_data = json.loads(json.loads(request.data.decode("utf-8")))
        filename = cli_data['filename']
        filesize = cli_data['filesize']

        # if file already exists, ERROR
        if filename in files_list:
            print("This file already exists!")
            return make_response(err_message.encode(), err_code)

        # else the file does not exist, create the DN list
        # 1: create list of block ids
        blockid_list = []
        block_index = 0
        filename = filename.replace("/", "")
        for i in range(0, filesize, block_size):
            blockid_list.append(filename + "_b" + str(block_index))
            block_index += 1

        # 2: assign a list of DN to each block (round robin) + create an empty DN list to store locally
        block_json = {}                                         # "inside" json for block data
        block_json_emptylist = {}                               # empty DN list for NN to store
        rr_index = 0                                            # round robin index
        empty_str = ""                                          # for block_json_emptyList

        for block in blockid_list:                              # make a DN list for each blockid in file
            dn_str = ""
            dn_ip_list = []
            for i in range(0, replication_factor):              # assign N # of DNs per blockid
                dn_ip_list = list(master_heartbeat_dict.keys())
                ip = dn_ip_list[(rr_index + i) % len(dn_ip_list)] # round robin assignment
                dn_str = dn_str + ip + " "

            rr_index = (rr_index + 1) % len(dn_ip_list)         # increment the rr_index or wraps back around
            block_json.update({block: dn_str})                  # update block_json for client
            block_json_emptylist.update({block: empty_str})     # update block_json_emptyList to store locally

        # 3: create the final DN list to send to client + store the empty DN list version locally in master DN list
        DN_list_json = {filename: block_json_emptylist}
        master_DNlists_dict.update(DN_list_json)                # store the local version

        DN_list_json_cli = {filename: block_json}

        print("\nSending DN list to client...")
        return make_response(json.dumps(DN_list_json_cli), 200)
        # return json.dumps(DN_list_json_cli)                    # send the client version


class BlockBeats(Resource):

    # For DN's block report / heart beat.
    # DN will POST its address and a list of block ids. Use this info to update master DN list.
    def post(self):

        bb = json.loads(request.data.decode("utf-8"))           # POST data from DN
        sender_addr = "http://" + request.environ['REMOTE_ADDR'] + ":" + str(port)
        block_list = bb["block_report"]                         # get list of blocks that this DN currently has

        # update master heart beat list with DN's IP and current time stamp
        master_heartbeat_dict.update({sender_addr: datetime.datetime.now()})

        # update master DN list
        for dn_b in block_list:
            for f in master_DNlists_dict:
                for b in master_DNlists_dict[f]:
                    ip_list = master_DNlists_dict[f][b].split()
                    if b == dn_b and sender_addr not in ip_list:
                        master_DNlists_dict[f][b] = master_DNlists_dict[f][b] + " " + sender_addr + " "

        print("\033[94mMASTER LIST\033[0m")
        for f in master_DNlists_dict:
            print("File: ", f)
            for b in master_DNlists_dict[f]:
                print("\tBlock: ", b, " --> ", master_DNlists_dict[f][b])
        print()


# -----------------------------------------------------
# THREAD - CHECKS BB LIST AND MAINTAINS FAULT TOLERANCE
# -----------------------------------------------------


def interrupt():
    global yourThread
    yourThread.cancel()


# NN CHECKING IF A DN HAS TIMED OUT - check bb table and removes from master DN list if failure
# Checks block beat table to see if a blockid has not sent a block report in 30 seconds
def check_bb_table():

    print("\033[92mBLOCK BEAT THREAD:\033[0m")
    # Do initialisation stuff here
    global yourThread

    # Send block report to NN
    with DataLock:

        failed_DN_list = []

        # find nodes that have failed
        for DN_addr in master_heartbeat_dict.keys():
            elapsed = datetime.datetime.now() - master_heartbeat_dict[DN_addr]
            print(DN_addr, " time elapsed -- ", elapsed)
            if elapsed > datetime.timedelta(seconds=wait_time):
                print("\033[91m --> DN FAILURE: ", DN_addr, " has failed! No block report for time: ",elapsed,"\033[0m")
                failed_DN_list.append(DN_addr)
        print()

        # remove failed DNs from BB list
        for DN in failed_DN_list:
            del master_heartbeat_dict[DN]

        # update master DN list
        seperator = " "
        for DN_addr in failed_DN_list:
            for f in master_DNlists_dict:                                       # check every file
                for b in master_DNlists_dict[f]:                                # check every block of a file
                    ip_list = master_DNlists_dict[f][b].split()

                    if DN_addr in ip_list:                                      # check if blockid string is in ip str
                        ip_list.remove(DN_addr)                                 # remove the addr from the ip str
                        new_ip_str = seperator.join(ip_list)
                        master_DNlists_dict[f][b] = new_ip_str
                        print("Master List after Failed DN removal: ")
                        for f1 in master_DNlists_dict:
                            print("File: ", f1)
                            for b1 in master_DNlists_dict[f1]:
                                print("\tBlock: ", b1, " --> ", master_DNlists_dict[f1][b1])
                        print()

                        available_DNs_list = list(set(master_heartbeat_dict.keys()) - set(ip_list))
                        if len(available_DNs_list) == 0:
                            print("ERROR: Not enough DNs in the system! Quitting.")
                            return "ERROR"

                        sender_DN = ip_list[0]                                  # hard coded to get first DN  ??
                        recv_DN = available_DNs_list[0]                         # hard coded to get first DN  ??
                        print("Maintaining Replication factor -- sender DN: ", sender_DN, end="")
                        print(" -- recv DN: ", recv_DN, end="")
                        print(" -- block: ", b, "\n")
                        sender_DN_endpoint_addr = sender_DN + fault_tolerance
                        data = json.dumps({b: recv_DN})
                        response = requests.post(sender_DN_endpoint_addr, json=data)


    yourThread = threading.Timer(wait_time, check_bb_table)
    yourThread.start()


check_bb_table()                                # Initialize blockBeat thread
atexit.register(interrupt)                      # When you kill Flask (SIGTERM), clear the trigger for the next thread


api.add_resource(NN_server, "/")
api.add_resource(BlockBeats, "/BB")


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=port)
