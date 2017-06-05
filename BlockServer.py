#!/usr/bin/env python

import sys

sys.path.append('gen-py')

from blockServer import BlockServerService
from blockServer.ttypes import *
from shared.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.server import TServer
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

class BlockServerHandler():

    def __init__(self, config_path):
        # Initialize using config file, intitalize state etc
        self.configFile = config_path
        self.hashToChunkMap = {}

    def storeBlock(self, hashBlock):
        # Store hash block, called by client during upload
        self.hashToChunkMap[hashBlock.hash] = hashBlock.block
        resp = response()
        resp.message = responseType.OK
        print "BlockServer: blocks stored"
        return resp

    def getBlocks(self, hash):
        # Retrieve block using hash, called by client during download
        hashBlocks = hashBlocks()
        allOk = True
        for hashString in hash:
            hashblock = hashblock()
            if hashString in self.hashToChunkMap:
                hashblock.hash = hashString
                hashblock.block = self.hashToChunkMap[hashString] 
                hashblock.status = "OK" 
            else:
                hashblock.status = "ERROR"
                allOk = False
                print "BlockServer: Blocks missing"
            hashBlocks.blocks.append(hashblock)
            if allOK == False:
                break
        if allOk == True:
            print "BlockServer: all blocks found"
        return hashBlocks


    def deleteBlock(self, hash):
        # Delete the particular hash : block pair
        pass

    def hasBlocks(self, hashList):
        uResponse = uploadResponse()
        uResponse.status = uploadResponseType.OK
        uResponse.hashList = []
        print "hasBlock invoked"
        for hashV in hashList:
            if hashV not in self.hashToChunkMap:
                uResponse.status = uploadResponseType.MISSING_BLOCKS
                uResponse.hashList.append(hashV)
                print "Meta -> Block: block missing"
        return uResponse


    def readServerPort(self):
        # In this function read the configuration file and get the port number for the server
        file = open(self.configFile, 'r')
        while True:
            line = file.readline()
            if not line:
                break
            words = line.split(':')
            if words[0] == "block":
                blockPort = words[1].strip()
        file.close()
        return blockPort

    # Add your functions here

# Add additional classes and functions here if needed


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print "Invocation <executable> <config_file>"
        exit(-1)

    config_path = sys.argv[1]

    print "Initializing block server"
    handler = BlockServerHandler(config_path)
    # Retrieve the port number from the config file so that you could strt the server
    port = handler.readServerPort()
    # Define parameters for thrift server
    processor = BlockServerService.Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    # Create a server object
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    print "Starting server on port : ", port

    try:
        server.serve()
    except (Exception, KeyboardInterrupt) as e:
        print "\nExecption / Keyboard interrupt occured: ", e
        exit(0)
