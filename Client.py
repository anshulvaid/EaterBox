#!/usr/bin/env python

import sys
import os, math, hashlib

sys.path.append('gen-py')

# Thrift specific imports
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from shared.ttypes import *
from metadataServer.ttypes import *
from blockServer.ttypes import *
from metadataServer import MetadataServerService
from blockServer import BlockServerService
# Add classes / functions as required here

my_id = ""
configFile = ""
blockServerPort = 0
metaDataServerPorts = []
globalHashToChunkMap = {}

def init(config_path, config_file):
        # Initialize block
    global globalHashToChunkMap, configFile, my_id, blockServerPort, metaDataServerPorts
    my_id = config_path
    configFile = config_file
    blockServerPort, metaDataServerPorts = getPorts()
    for filename in os.listdir(config_path):
        localChunkHashList, localHashToChunkMap = divideAndStoreChunks(config_path + "/" + filename)
        globalHashToChunkMap.update(localHashToChunkMap)

def getPorts():
    global configFile
    file = open(configFile, 'r')
    metaDataPorts = []
    while True:
        line = file.readline()
        if not line:
            break
        words = line.split(':')
        if "metadata" in words[0]:
            metaDataPorts.append(words[1].strip())
        elif words[0] == "block":
            blockPort = words[1].strip()
    file.close()
    return blockPort, metaDataPorts

def divideAndStoreChunks(filename):
    file = open(filename, 'r')
    chunkHashList = []
    hashToChunkMap = {}
    while True:
        chunk = file.read(4 * int(math.pow(2,20)))
        if chunk:
            hashed = hashlib.sha256(chunk)
            hexValue = hashed.hexdigest()
            chunkHashList.append(hexValue)
            hashToChunkMap[hexValue] = chunk
        else:
            break
    file.close()
    return chunkHashList, hashToChunkMap

def getConnection(portNumber, serverType):
    transport = TSocket.TSocket('localhost', portNumber)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    if serverType =="meta":
        client = MetadataServerService.Client(protocol)
    elif serverType == "block":
        client = BlockServerService.Client(protocol)

    try:
        transport.open()
    except Exception as e:
        print "Error while opening socket to server\n", e
        exit(1)

    return client, transport

def uploadFile(filename):
    #connect to MetaDataServer
    global my_id, metaDataServerPorts, blockServerPort
    filename = my_id + "/" + filename
    MetaHandler, transportM = getConnection(metaDataServerPorts[0], "meta")

    thisFile = file()
    thisFile.filename = filename
    thisFile.hashList, fileHashToChunkMap = divideAndStoreChunks(filename)
    thisFile.status = responseType.OK
    uResponse = uploadResponse()
    uResponse = MetaHandler.storeFile(thisFile)

    rsp = response()
    rsp.message = responseType.OK
    if uResponse.status == uploadResponseType.OK or uResponse.status == uploadResponseType.FILE_ALREADY_PRESENT:
        rsp.message = responseType.OK
    elif uResponse.status == uploadResponseType.MISSING_BLOCKS:
        BlockHandler, transportB = getConnection(blockServerPort, "block")
        for hashval in uResponse.hashList:
            hashBlk = hashBlock()
            hashBlk.hash = hashval
            hashBlk.block = fileHashToChunkMap[hashval]
            resp = BlockHandler.storeBlock(hashBlk)
            if resp.message == responseType.ERROR:
                rsp.message = responseType.ERROR
            else:
                print "Client: All blocks stored on BlockServer"
        transportB.close()
        if rsp.message == responseType.OK:
            uResponse = MetaHandler.storeFile(thisFile)
            if uResponse.status == uploadResponseType.ERROR:
                rsp.message = responseType.ERROR
            else:
                print "Client: Metadata stored"
    else:
        rsp.message = responseType.ERROR
    
    transportM.close()
    return rsp

def mergeChunksAndSaveFile(fileHashList, filename):
    global my_id,globalHashToChunkMap
    #newFile = open("client11"+'/'+filename, 'w')
    newFile = open(filename, 'w')
    for hashValue in fileHashList:
        newFile.write(globalHashToChunkMap[hashValue])
    newFile.close()
    print "File " + filename + " successfully downloaded."


def getFile(filename):
    global my_id, metaDataServerPorts, blockServerPort,globalHashToChunkMap
    MetaHandler, transportM = getConnection(metaDataServerPorts[0], "meta")
    filename = my_id + "/" + filename
    downFile = MetaHandler.getFile(filename)
    transportM.close()
    if downFile.status == responseType.OK:
        missingChunkList = []
        allOk = True
        for hashValue in downFile.hashList:
            if hashValue not in globalHashToChunkMap:
                missingChunkList.append(hashValue)
        if missingChunkList:
            BlockHandler, transportB = getConnection(blockServerPort, "block")
            missingChunks = transportB.getBlocks(missingChunkList)
            transportB.close()
            for hashblock in missingChunks.blocks:
                if hashBlock.status != "OK":
                    print "Download file failed: file content incomplete"
                    allOk = False
                globalHashToChunkMap[hashBlock.hash] = hashBlock.block
        if allOk == True:
            mergeChunksAndSaveFile(downFile.hashList, filename)
    else:
        print "Download file failed: file not found"
    
def deleteFile(filename):
    global my_id, metaDataServerPorts
    filename = my_id + "/" + filename
    MetaHandler, transportM = getConnection(metaDataServerPorts[0], "meta")

    thisFile = file()
    thisFile.filename = filename
    thisFile.status = responseType.OK

    response = MetaHandler.deleteFile(thisFile)
    if response.message == responseType.OK:
        os.remove(filename)
        print "File successfully deleted."
    else:
        print "File deletion failed."
    transportM.close()

if __name__ == "__main__":

    if len(sys.argv) < 5:
        print "Invocation : <executable> <config_file> <base_dir> <command> <filename>"
        exit(-1)


    print "Starting client"

    base_dir = sys.argv[2]
    config_file = sys.argv[1]
    command = sys.argv[3]
    fileName = sys.argv[4]

    init(base_dir, config_file)

    if command.lower() == "upload": 
        resp = uploadFile(fileName)
        if resp.message == uploadResponseType.OK:
            print "File " + fileName + " successfully uploaded."
    elif command.lower() == "download":
        getFile(fileName)
    elif command.lower() == "delete":
        deleteFile(fileName)

    '''
    Server information can be parsed from the config file

    connections can be created as follows

    Eg:

    # Make socket
    transport = TSocket.TSocket('serverip', serverport)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = HelloService.Client(protocol)

    # Connect!
    try:
        transport.open()
    except Exception as e:
        print "Error while opening socket to server\n", e
        exit(1)

    # Create custom data structure object
    m = message()

    # Fill in data
    m.data = "Hello From Client!!!"

    # Call via RPC
    try:
        dataFromServer = client.HelloServiceMethod(m)
    except Exception as e:
        print "Caught an exception while calling RPC"
        # Add handling code
        exit(1)

    '''
