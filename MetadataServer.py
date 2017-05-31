#!/usr/bin/env python

import sys
import collections

sys.path.append('gen-py')

# Thrift specific imports
from thrift import Thrift
from thrift.transport import TSocket
from thrift.server import TServer
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

# Protocol specific imports
from metadataServer import MetadataServerService
from blockServer import BlockServerService
from shared.ttypes import *

class MetadataServerHandler():

    blockPort = 0
    def __init__(self, config_path, my_id):
        self.configFile = config_path
        self.serverId = my_id
        self.fileNameToHashList = {}

    def getConnection(self, portNumber, serverType):
        transport = TSocket.TSocket('localhost', portNumber)

        # Buffering is critical. Raw sockets are very slow
        transport = TTransport.TBufferedTransport(transport)

        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        if serverType =="block":
            client = BlockServerService.Client(protocol)
        else:
            client = MetadataServerService.Client(protocol)
        
        try:
            transport.open()
        except Exception as e:
            print "Error while opening socket to server\n", e
            exit(1)

        return client, transport

    def getFile(self, filename):
        # Function to handle download request from file
        resp = file()
        if filename in self.fileNameToHashList:
        	resp.filename = filename
        	resp.hashList = self.fileNameToHashList[filename]
        	resp.status = responseType.OK
        	print "MetaServer: file "+ filename + " found and all OK."
        else:
        	resp.status = responseType.ERROR
        	print "MetaServer: file " + filename + " not found."
        return resp

    def storeFile(self, file):
        # Function to handle upload request
        global blockPort
        resp = uploadResponse()

        #TODO: check if file already exists

        BlockHandler, transportB = self.getConnection(blockPort, "block")
        uResponse = BlockHandler.hasBlocks(file.hashList)
        transportB.close()
        if uResponse.status == uploadResponseType.OK:
            resp.status = uploadResponseType.OK
            self.fileNameToHashList[file.filename] = file.hashList
            print "MetaServer: All OK for file, " + file.filename
        else:
            resp.status = uploadResponseType.MISSING_BLOCKS
            resp.hashList = uResponse.hashList
            print "MetaServer: Missing blocks for file, " + file.filename
        return resp

    def deleteFile(self, filename):
        # Function to handle download request from file
        pass

    def readServerPort(self):
        global blockPort
        file = open(self.configFile, 'r')
        while True:
            line = file.readline()
            if not line:
                break
            words = line.split(':')
            if words[0] == "metadata"+ self.serverId :
                metaPort = words[1].strip()
            elif words[0] == "block":
                blockPort = words[1].strip()
        file.close()
        return metaPort

    # Add other member functions if needed

# Add additional classes and functions here if needed

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print "Invocation <executable> <config_file> <id>"
        exit(-1)

    config_path = sys.argv[1]
    my_id = sys.argv[2]

    print "Initializing metadata server"
    handler = MetadataServerHandler(config_path, my_id)
    metaPort = handler.readServerPort()

    # Define parameters for thrift server
    processor = MetadataServerService.Processor(handler)
    transport = TSocket.TServerSocket(port=metaPort)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    # Create a server object
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    print "Starting server on port : ", metaPort

    try:
        server.serve()
    except (Exception, KeyboardInterrupt) as e:
        print "\nExecption / Keyboard interrupt occured: ", e
        exit(0)
