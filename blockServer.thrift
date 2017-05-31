include "shared.thrift"

namespace cpp blockServer
namespace py blockServer
namespace java blockServer

/* The status field can be used to communicate state information
	example, when the client requests a block that is not present
	you can set the status as ERROR */

typedef shared.response response
typedef shared.uploadResponse uploadResponse

struct hashBlock {
	1: string hash,
	2: binary block,
	3: string status
}

struct hashBlocks {
	1: list<hashBlock> blocks
}

service BlockServerService {
	response storeBlock(1: hashBlock hashblock),
	hashBlocks getBlocks(1: list<string> hash),
	response deleteBlock(1: string hash)
	uploadResponse hasBlocks(1: list<string> hashList)
	// Add any procedure you need below

}
