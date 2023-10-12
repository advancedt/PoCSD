import pickle, logging
import fsconfig

#### BLOCK LAYER

# global TOTAL_NUM_BLOCKS, BLOCK_SIZE, INODE_SIZE, MAX_NUM_INODES, MAX_FILENAME, INODE_NUMBER_DIRENTRY_SIZE

class DiskBlocks():
    def __init__(self):

        # block[] is a list that stores all the blocks that make up the raw block layer
        # the list has TOTAL_NUM_BLOCKS entries
        # each entry in block[] is a bytearray with size BLOCK_SIZE
        # a block can be indexed by its block number
        self.block = []

        # Initialize all raw blocks with zeros
        for i in range (0, fsconfig.TOTAL_NUM_BLOCKS):
            # putdata is a bytearray initialized with zeroes
            putdata = bytearray(fsconfig.BLOCK_SIZE)
            self.block.insert(i,putdata)

    ## Put: interface to write a raw block of data to the block indexed by block number
    ## Blocks are padded with zeroes up to BLOCK_SIZE

    def Put(self, block_number, block_data):

        logging.debug(
            'DiskBLocks::Put: block number ' + str(block_number) + ' len ' + str(len(block_data)) + '\n' + str(block_data.hex()))

        if len(block_data) > fsconfig.BLOCK_SIZE:
            logging.error('DiskBlocks::Put: Block larger than BLOCK_SIZE: ' + str(len(block_data)))
            quit()

        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            # block_data has the data to put in the block
            # if it's not as large as a full block, ljust pads it with zeros
            putdata = bytearray(block_data.ljust(fsconfig.BLOCK_SIZE, b'\x00'))
            # A full block padded with zeroes is now in putdata; write it to the block[] list
            self.block[block_number] = putdata
            # Success
            return 0
        else:
            logging.error('DiskBlocks::Put: Block out of range: ' + str(block_number))
            quit()


    ## Get: interface to read a raw block of data from block indexed by block number
    ## Equivalent to the textbook's BLOCK_NUMBER_TO_BLOCK(b)

    def Get(self, block_number):

        # Block must be within range
        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            logging.debug ('DiskBlocks::Get: block number ' + str(block_number) + '\n' + str((self.block[block_number]).hex()))
            return self.block[block_number]

        logging.error('DiskBlocks::Get: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()


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
