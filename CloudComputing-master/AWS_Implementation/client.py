import requests
import boto3
import simplejson as json
import base64
from botocore.exceptions import ClientError


# Global variables
NN_addr = "http://18.236.118.187:5000"                                                    # TODO: insert NN IP ADDR HERE
s3 = boto3.resource('s3')                                                   # for accessing s3 on a write
block_size = 64000000                                                       # 64 MB
replication_factor = 3
err = "ERROR"


def greetings():
    print("\n---------------------------------------------")
    print("Welcome to the Dunder Mifflin Client Program!")
    print("---------------------------------------------\n")


def bye():
    print("\nThanks for visiting Dunder Mifflin. Bye!\n")


def action_list():
    options = """\nChoose an action 1-4:\n
    1: Create/write file in SUFS
    2: Read file
    3: List Data Nodes that store replicas of each block of file
    4: Exit program\n"""
    print(options)

    selection = input("Please choose an action 1-4: ")

    while selection not in ('1', '2', '3', '4'):
        selection = input("Please choose an action 1-4: ")

    return selection


"""
WRITE

Gets the filename from user. 
POSTS the filename and size to NN and gets DN list returned. 
Uses DN list to send blocks of data to DNs. 
"""
def write_file():

    print("\n\033[91m------")
    print("WRITE")
    print("------\033[0m\n")

    # get S3 object path from user
    filename = input("Enter an S3 object path: ")                           # s3 bucket name: dundermifflin-sufs

    path_list = filename.split("/", 1)                                      # check that a bucket and file were given
    if len(path_list) < 2:
        print("You must include the bucket and file.")
        return

    bucket = path_list[0]
    filename = path_list[1]
    print("bucket ", bucket)
    print("path   ", filename)
    s3obj = s3.Object(bucket, filename)                                     # var that represents an s3 object

    try:
        print("Reading S3 object ...")
        image = s3obj.get()['Body'].read()

    except ClientError as ex:
        print("ERROR: the s3 file path you entered is not valid.", ex)      # return if given invalid s3 file path
        return

    # Save save file name and file size into json object
    size = s3obj.content_length
    file_dict = {"filename": filename, "filesize": size}                    # json object with file name and file size
    data_json = json.dumps(file_dict)                                       # convert file info dict into json

    # Send json object to NameNode and get DN list back as a response
    print("Sending file info for WRITE to Name Node:")
    print("  - File name: ", filename)
    print("  - File size: ", size, "\n")
    response = POST(data_json, NN_addr)                                     # POST the file name and size to NN

    # check if file already exists (if exists, print error and return)
    if response is err:
        print("ERROR: cannot write ", filename, "because it already exists.")
        return

    # Else, forward block data to each DN in the DN List
    print("File info sent to NN.")
    print("NN returned DN list for file:", filename)
    print("Sending file blocks to DNs...\n")

    my_DN_dict = json.loads(response)                                       # DN list as a dict
    file_in_blocks = get_file_list_in_blocks(image)

    i = 0                                                                   # index of file_in_blocks
    # loop through DN_list and send each block to the given DN
    for f in my_DN_dict:
        for b in my_DN_dict[f]:
            print("\nSending block: ", b)
            block_for_DN = {b: base64.b64encode(file_in_blocks[i])}         # get next chunk of file
            i = i + 1
            dn_dest_list = my_DN_dict[f][b].strip(" ").split(" ")           # convert DN str to DN list

            # for each DN in the DN list, send {blockid: data}
            for dn in dn_dest_list:

                print(dn, " ---> ", b)                                      # dn represents the ip:port of DN
                data = block_for_DN
                POST(data, dn)


def get_file_list_in_blocks(file):

    L = [file[i:i + block_size] for i in range(0, len(file), block_size)]
    return L



"""
WRITE's helper function

Takes a file as a string as a parameter. 
Breaks the file into "block-sized" chunks into a list. 
Returns list. 
"""
def get_file_in_blocks(file_str):

    file_in_blocks = []

    for block in range(0, len(file_str), block_size):                       # break file into N sized chunks
        str = file_str[block:block+block_size]
        file_in_blocks.append(str)

    return file_in_blocks


"""
READ

Calls get_DN_list helper function which returns DN list or "ERROR". 
If DN list returned, get block data from DN in DN list. 
"""
def read_file():

    dn_list, file = get_DN_list()                                           # get DN list or ERROR if does not exist

    if dn_list == err:
        print("ERROR: Not a valid file for reading.")
        return

    print("\n\033[94m--------------------------------------------")
    print("READ FILE: ", file)
    print("--------------------------------------------\033[0m")

    total_bytes = 0                                                         # track how many bytes are read

    read_file = open(file.replace("/", ""), "wb")                           # create file and save in local directory

    for block in dn_list:                                                   # Loop through each block in DN list
        uncleaned_list = dn_list[block].split(" ")
        ip_list = list(filter(None, uncleaned_list))                        # get DN nodes in list and filter
        print("\nBlock ", block, " should be on DNs: ", ip_list)

        i = 0                                                               # loop through each DN ip in the ip list
        while i < len(ip_list):
            dn = ip_list[i]
            payload = {"blockid": block}                                    # id of block that client is requesting
            response = GET(payload, dn)                                     # GET block from DN or err if does not exist
            response = base64.b64decode(response)

            # if you've looped through all dn and you still don't have the data... error!
            if response == err and i == (len(ip_list) - 1):
                print("ERROR: Missing a block of data! Failure of replication factor.")
                return

            # else if there's no error, you got the data - save and break to get next block
            elif response != err:
                print("Got block: ", block, "from data node: ", dn)
                read_file.write(response)
                total_bytes = total_bytes + len(response)                   # track total number of bytes written
                break

            # else, this block did not have the data
            else:
                i = i + 1

    read_file.close()
    print("\n\nRead of file", file, "complete!")
    print("Total bytes from READ: ", total_bytes, "\n")


"""
GET DN LIST 

Gets user input for file name. POSTS filename to NN to get DN list.
Return DN list as dict "ERROR".
"""
def get_DN_list():

    file = input("Enter the filename: ")                                # enter name of file to get DN list for
    data_json = {"filename": file}                                      # create the json object to POST
    response = GET(data_json, NN_addr)

    # if, NN returned an ERROR, return
    if response == err:
        return err, file

    # else, return the DN list as a dict
    else:
        return json.loads(response), file


"""
PRINT DN LIST

Calls get_DN_list helper function which returns "ERROR" or DN list. 
If DN list returned, print it out formatted. 
"""
def print_DN_list():

    # get DN list (or "ERROR" if NN does not have file)
    dn_list, file = get_DN_list()

    if dn_list == err:
        print("\nERROR: This file does not exist.")

    else:
        print("\n\033[92m--------------------------------------------")
        print("GET DN LIST FOR FILE: ", file)
        print("--------------------------------------------\033[m")

        # for each block in the file, print the DNs that holds this file
        for block in dn_list:
            print(block, " --> ", dn_list[block])
        print()


"""
GET help function. 
Client calls GET to either get DN list from NN or to get block of data from DN. 

Parameters: 
- data: data to send with get request
- addr: address to send request to
"""
def GET(data, addr):
    response = requests.get(addr, params=data)                              # send data to addr

    if response.status_code != 200:
        print("GET ERROR: ", response.status_code)
        return err

    else:
        return response.content


"""
POST helper function. 
Client calls POST to NN or DN. 

Parameters: 
- data: data to send with post request
- addr: address to send request to
"""
def POST(data, addr):
    response = requests.post(addr, json=data)                               # send data to addr

    if response.status_code != 200:
        print("POST ERROR: ", response.status_code)
        return err

    else:
        return response.content.decode("utf-8")


"""                     
  __  __   _   ___ _  _ 
 |  \/  | /_\ |_ _| \| |
 | |\/| |/ _ \ | || .` |
 |_|  |_/_/ \_\___|_|\_|
                        
"""


def main():

    # Loop until user quits with action #4
    while True:

        # print action selection list
        action = action_list()

        if action is "1":
            write_file()

        elif action is "2":
            read_file()

        elif action is "3":
            print_DN_list()

        else:
            break

    # Quit program
    bye()


if __name__ == "__main__":
    main()
