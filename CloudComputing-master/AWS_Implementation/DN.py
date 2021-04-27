from flask import Flask, make_response
from flask_restful import Api, Resource, reqparse, request
import simplejson as json
import threading
import requests
import atexit

app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('file')

# Global variables
NN_addr = "http://:5000"                                               # TODO: insert NN IP ADDR HERE
port = 5000
blockbeat = "/BB"
err_code = 400
err_message = "ERROR"
fault_tolerance = "/FT"                                                 # listen for POSTs from NN

# My data
my_blocks = []

# Threading variables
dataLock = threading.Lock()                                             # Lock to control access to data
yourThread = threading.Thread()                                         # Thread handler
wait_time = 7                                                           # Time between blockbeats


class data_from_NN(Resource):
    def post(self):

        with dataLock:
            blockid = ""
            addr = ""
            data = json.loads(json.loads(request.data.decode("utf-8")))
            for key, value in data.items():
                blockid = key
                addr = value

            with open(blockid, 'r') as myfile:
                data = myfile.read()
            data_for_DN = {blockid: data}

            print("blockid:      ", blockid)
            print("addr:         ", addr)
            print("sending data of type ", type(data_for_DN))

            response = requests.post(addr, json=data_for_DN)            # send data to addr

            if response.status_code != 200:
                print("POST ERROR: ", response.status_code)


class DN_server(Resource):

    def get(self):
        # Get block id sent from client (key = "blockid")
        parser = reqparse.RequestParser()
        parser.add_argument("blockid")                                  # Name of key
        args = parser.parse_args()
        blockid = args["blockid"]                                       # Payload from client containing block id

        print("\nClient requested block: ", blockid, " - checking if I have it... ", end="")

        with dataLock:

            # If I have the block id, send the data back
            if blockid in my_blocks:
                print("I HAVE block:", blockid, "\n")

                with open(blockid, 'r') as myfile:
                    data = myfile.read()
                return make_response(data, 200)

            # Else, return ERROR
            else:
                print("I do NOT have block:", blockid, "\n")
                return err_message


    def post(self):

        with dataLock:

            # get the block id and use as filename
            block_data = json.loads(request.data)
            for blockid, data in block_data.items():
                my_blocks.append(blockid)
                file = open(blockid, "w")                               # write block data into file
                file.write(data)
                file.close()

            # Test print
            print("My blocks: ")
            for blockid in my_blocks:
                print(blockid)
            print()


def interrupt():
    global yourThread
    yourThread.cancel()


def blockBeat():
    global yourThread                                                   # Initialize here

    # Send block report to NN
    with dataLock:
        NN_BB_addr = NN_addr + blockbeat                                # Address of NN + block beat end point --> "/BB"
        block_report = {"block_report": my_blocks}
    response = requests.post(NN_BB_addr, json=block_report)             # Send my blocks as a list to NN

    if response.status_code != 200:
        print("ERROR: Error in sending block report to NN")
    else:
        print("SUCCESS: Sent block report to NN\n")

    yourThread = threading.Timer(wait_time, blockBeat)
    yourThread.start()


blockBeat()                                                             # Initialize blockBeat thread
atexit.register(interrupt)                                              # When you kill Flask (SIGTERM),
                                                                        # clear the trigger for the next thread

api.add_resource(DN_server, "/")
api.add_resource(data_from_NN, fault_tolerance)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=port)
