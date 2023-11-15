import pickle, logging
import fsconfig
import xmlrpc.client, socket, time

#### BLOCK LAYER

# global TOTAL_NUM_BLOCKS, BLOCK_SIZE, INODE_SIZE, MAX_NUM_INODES, MAX_FILENAME, INODE_NUMBER_DIRENTRY_SIZE

class DiskBlocks():
    def __init__(self):

        # initialize clientID
        if fsconfig.CID >= 0 and fsconfig.CID < fsconfig.MAX_CLIENTS:
            self.clientID = fsconfig.CID
        else:
            print('Must specify valid cid')
            quit()

        # initialize XMLRPC client connection to raw block server
        if fsconfig.PORT:
            PORT = fsconfig.PORT
        else:
            print('Must specify port number')
            quit()
        server_url = 'http://' + fsconfig.SERVER_ADDRESS + ':' + str(PORT)
        self.block_server = xmlrpc.client.ServerProxy(server_url, use_builtin_types=True)
        socket.setdefaulttimeout(fsconfig.SOCKET_TIMEOUT)

        # 实现一个字典缓存
        self.cacheDict = {}

    def checkCid(self):
        # 如果当前服务器的cid与客户端的cid不同
        if self.clientID != bytearray(self.Get(fsconfig.TOTAL_NUM_BLOCKS - 2))[0]:
            # 使缓存无效
            self.cacheDict = {}
            print("CACHE_INVALIDATED")
            cid = bytearray(self.clientID.to_bytes(length=fsconfig.BLOCK_SIZE, byteorder='little'))
            # 写入当前cid
            self.Put(fsconfig.TOTAL_NUM_BLOCKS - 2, cid)
            print("CACHE_WRITE_THROUGH" ,fsconfig.TOTAL_NUM_BLOCKS - 2)

    def putCid(self):
        cid = bytearray(self.clientID.to_bytes(length=fsconfig.BLOCK_SIZE, byteorder='little'))
        self.Put(fsconfig.TOTAL_NUM_BLOCKS-2, cid)


    def Put(self, block_number, block_data):

        logging.debug(
            'Put: block number ' + str(block_number) + ' len ' + str(len(block_data)) + '\n' + str(block_data.hex()))
        if len(block_data) > fsconfig.BLOCK_SIZE:
            logging.error('Put: Block larger than BLOCK_SIZE: ' + str(len(block_data)))
            quit()

        #实现至少写一次
        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            putdata = bytearray(block_data.ljust(fsconfig.BLOCK_SIZE, b'\x00'))
            execute = False
            while (not execute):
                try:
                    # 更新服务器
                    ret = self.block_server.Put(block_number, putdata)
                    execute = True
                except socket.timeout:
                    print("SERVER_TIMED_OUT")
            # 如果不是更新cid操作
            if block_number!=fsconfig.TOTAL_NUM_BLOCKS - 2:
                self.putCid()
                # 更新缓存
                self.cacheDict[block_number] = putdata
                print("CACHE_WRITE_THROUGH " + str(block_number))

            if ret == -1:
                logging.error('Put: Server returns error')
                quit()
            return 0
        else:
            logging.error('Put: Block out of range: ' + str(block_number))
            quit()


    ## Get: interface to read a raw block of data from block indexed by block number
    ## Equivalent to the textbook's BLOCK_NUMBER_TO_BLOCK(b)

    def Get(self, block_number):

        logging.debug('Get: ' + str(block_number))
        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            #首先从缓存里寻找所要信息
            #命中缓存，直接从缓存里返回
            if block_number in self.cacheDict:
                print("CACHE_HIT "+ str(block_number))
                return self.cacheDict[block_number]
            # 没有命中缓存
            print("CACHE_MISS " + str(block_number))
            # 要从内存中取出block[block_number]的内容，同时将其装入cache
            # 实现至少写一次
            execute = False
            while (not execute):
                try:
                    # 从服务器里找
                    data = self.block_server.Get(block_number)
                    execute = True
                except socket.timeout:
                    print("SERVER_TIMED_OUT")
            if block_number < fsconfig.TOTAL_NUM_BLOCKS-2:
                # 将数据装入cache
                self.cacheDict[block_number] = bytearray(data)

            return bytearray(data)

        logging.error('DiskBlocks::Get: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()

    def RSM(self, block_number):
        logging.debug('RSM: ' + str(block_number))
        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            execute = False
            while (not execute):
                try:
                    data = self.block_server.RSM(block_number)
                    execute = True
                except socket.timeout:
                    print("SERVER_TIMED_OUT")
            return bytearray(data)
        logging.error('DiskBlocks::RSM: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()

    def Acquire(self):
        logging.debug('Acquire')
        lock = self.RSM(fsconfig.TOTAL_NUM_BLOCKS-1)[0]
        while lock == 1:
            time.sleep(fsconfig.RETRY_INTERVAL)
            lock = self.RSM(fsconfig.TOTAL_NUM_BLOCKS-1)[0]
        self.checkCid()
        return 0

    def Release(self):
        logging.debug('Acquire')
        RSM_UNLOCKED = bytearray(b'\x00') * 1
        self.Put(fsconfig.TOTAL_NUM_BLOCKS-1, bytearray(RSM_UNLOCKED.ljust(fsconfig.BLOCK_SIZE, b'\x00')))
        return 0


    ## Serializes and saves the DiskBlocks block[] data structure to a "dump" file on your disk

    def DumpToDisk(self, filename):

        logging.info("DiskBlocks::DumpToDisk: Dumping pickled blocks to file " + filename)
        file = open(filename,'wb')
        file_system_constants = "BS_" + str(fsconfig.BLOCK_SIZE) + "_NB_" + str(fsconfig.TOTAL_NUM_BLOCKS) + "_IS_" + str(fsconfig.INODE_SIZE) \
                            + "_MI_" + str(fsconfig.MAX_NUM_INODES) + "_MF_" + str(fsconfig.MAX_FILENAME) + "_IDS_" + str(fsconfig.INODE_NUMBER_DIRENTRY_SIZE)
        pickle.dump(file_system_constants, file)
        pickle.dump(self.block, file)

        file.close()

    ## Loads DiskBlocks block[] data structure from a "dump" file on your disk

    def LoadFromDump(self, filename):

        logging.info("DiskBlocks::LoadFromDump: Reading blocks from pickled file " + filename)
        file = open(filename,'rb')
        file_system_constants = "BS_" + str(fsconfig.BLOCK_SIZE) + "_NB_" + str(fsconfig.TOTAL_NUM_BLOCKS) + "_IS_" + str(fsconfig.INODE_SIZE) \
                            + "_MI_" + str(fsconfig.MAX_NUM_INODES) + "_MF_" + str(fsconfig.MAX_FILENAME) + "_IDS_" + str(fsconfig.INODE_NUMBER_DIRENTRY_SIZE)

        try:
            read_file_system_constants = pickle.load(file)
            if file_system_constants != read_file_system_constants:
                print('DiskBlocks::LoadFromDump Error: File System constants of File :' + read_file_system_constants + ' do not match with current file system constants :' + file_system_constants)
                return -1
            block = pickle.load(file)
            for i in range(0, fsconfig.TOTAL_NUM_BLOCKS):
                self.Put(i,block[i])
            return 0
        except TypeError:
            print("DiskBlocks::LoadFromDump: Error: File not in proper format, encountered type error ")
            return -1
        except EOFError:
            print("DiskBlocks::LoadFromDump: Error: File not in proper format, encountered EOFError error ")
            return -1
        finally:
            file.close()


## Prints to screen block contents, from min to max

    def PrintBlocks(self,tag,min,max):
        print ('#### Raw disk blocks: ' + tag)
        for i in range(min,max):
            print ('Block [' + str(i) + '] : ' + str((self.Get(i)).hex()))
