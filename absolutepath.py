import fsconfig
import logging
from block import *
from inode import *
from inodenumber import *
from filename import *


## This class implements methods for absolute path layer

class AbsolutePathName():
    def __init__(self, FileNameObject, FileOperationsObject):
        self.FileNameObject = FileNameObject
        self.FileOperationsObject = FileOperationsObject
    def PathToInodeNumber(self, path, dir):

        logging.debug("AbsolutePathName::PathToInodeNumber: path: " + str(path) + ", dir: " + str(dir))

        if "/" in path:
            split_path = path.split("/")
            first = split_path[0]
            del split_path[0]
            rest = "/".join(split_path)
            logging.debug("AbsolutePathName::PathToInodeNumber: first: " + str(first) + ", rest: " + str(rest))
            d = self.FileNameObject.Lookup(first, dir)
            if d == -1:
                return -1
            return self.PathToInodeNumber(rest, d)
        else:
            return self.FileNameObject.Lookup(path, dir)

    def GeneralPathToInodeNumber(self, path, cwd):

        logging.debug("AbsolutePathName::GeneralPathToInodeNumber: path: " + str(path) + ", cwd: " + str(cwd))

        if path[0] == "/":
            if len(path) == 1:  # special case: root
                logging.debug("AbsolutePathName::GeneralPathToInodeNumber: returning root inode 0")
                return 0
            cut_path = path[1:len(path)]
            logging.debug("AbsolutePathName::GeneralPathToInodeNumber: cut_path: " + str(cut_path))
            return self.PathToInodeNumber(cut_path, 0)
        else:
            return self.PathToInodeNumber(path, cwd)

    def PathNameToInodeNumber(self, pathname, cwd):

        logging.debug("PathNameToInodeNumber::PathNameToInodeNumber: path: " + str(pathname) + ", cwd: " + str(cwd))

        i = self.GeneralPathToInodeNumber(pathname, cwd)
        # not a path
        if i == -1:
            return -1
        inodeObj = InodeNumber(i)
        inodeObj.InodeNumberToInode(self.FileNameObject.RawBlocks)
        # symlink
        if inodeObj.inode.type == fsconfig.INODE_TYPE_SYM:
            symBlock = self.FileNameObject.RawBlocks.Get(inodeObj.inode.block_numbers[0])
            # get the absolute path
            symPath = symBlock[0:inodeObj.inode.size]
            # get the inode number
            return self.PathNameToInodeNumber(symPath.decode(), cwd)
        # simple inode
        return i

    def Link(self, target, name, dir):
        logging.debug("FileOperations::Hardlink: dir: " + str(dir) + ", name: " + str(name))

        # Obtain dir_inode_number_inode, ensure it is a directory
        dir_inode = InodeNumber(dir)
        dir_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)
        # ERROR_LINK_NOT_DIRECTORY
        if dir_inode.inode.type != fsconfig.INODE_TYPE_DIR:
            logging.debug("ERROR_LINK_NOT_DIRECTORY" + str(dir))
            return -1, "ERROR_LINK_NOT_DIRECTORY"

        # Ensure target exists - if Lookup returns -1 it does not exist
        # target_inode = self.FileNameObject.Lookup(target, dir)
        target_inode = self.PathNameToInodeNumber(target, dir)
        # ERROR_LINK_TARGET_DOESNOT_EXIST
        if target_inode == -1:
            logging.debug("ERROR_LINK_TARGET_DOESNOT_EXIST " + str(name))
            return -1, "ERROR_LINK_TARGET_DOESNOT_EXIST"

        # Ensure target is a regular file
        target_file = InodeNumber(target_inode)
        target_file.InodeNumberToInode(self.FileNameObject.RawBlocks)
        # ERROR_LINK_TARGET_NOT_FILE
        if target_file.inode.type != fsconfig.INODE_TYPE_FILE:
            logging.debug("ERROR_LINK_TARGET_NOT_FILE " + str(name))
            return -1, "ERROR_LINK_TARGET_NOT_FILE"

        # Find available slot in directory data block
        fileentry_position = self.FileNameObject.FindAvailableFileEntry(dir)
        # ERROR_LINK_DATA_BLOCK_NOT_AVAILABLE
        if fileentry_position == -1:
            logging.debug("ERROR_LINK_DATA_BLOCK_NOT_AVAILABLE")
            return -1, "ERROR_LINK_DATA_BLOCK_NOT_AVAILABLE"

        # Ensure binding not exists - if Lookup returns -1 it does not exist
        file_inode = self.FileNameObject.Lookup(name, dir)
        # ERROR_LINK_ALREADY_EXISTS
        if file_inode != -1:
            logging.debug("ERROR_LINK_ALREADY_EXISTS " + str(name))
            return -1, "ERROR_LINK_ALREADY_EXISTS"

        # Create a hardlink here
        # inode_position = self.FileNameObject.FindAvailableInode()
        newfile_inode = InodeNumber(target_inode)
        newfile_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)
        newfile_inode.inode.refcnt += 1


        # Unlike DIRs, for FILES they are not allocated a block upon creatin; these are allocated on a Write()
        newfile_inode.StoreInode(self.FileNameObject.RawBlocks)

        # Add to parent's (filename,inode) table
        self.FileNameObject.InsertFilenameInodeNumber(dir_inode, name, target_inode)

        # Update directory inode
        # refcnt incremented by one
        dir_inode.inode.refcnt += 1
        dir_inode.StoreInode(self.FileNameObject.RawBlocks)

        return 0, "SUCCESS"


    def Symlink(self, target, name, dir):
        logging.debug("FileOperations::Softlink: dir: " + str(dir) + ", name: " + str(name))

        # Obtain dir_inode_number_inode, ensure it is a directory
        dir_inode = InodeNumber(dir)
        dir_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)
        # ERROR_SYMLINK_NOT_DIRECTORY
        if dir_inode.inode.type != fsconfig.INODE_TYPE_DIR:
            logging.debug("ERROR_SYMLINK_NOT_DIRECTORY" + str(dir))
            return -1, "ERROR_SYMLINK_NOT_DIRECTORY"

        # Ensure target exists - if Lookup returns -1 it does not exist
        # target_inode = self.FileNameObject.Lookup(target, dir)
        target_inode = self.PathNameToInodeNumber(target, dir)
        # ERROR_SYMLINK_TARGET_DOESNOT_EXIST
        if target_inode == -1:
            logging.debug("ERROR_SYMLINK_TARGET_DOESNOT_EXIST " + str(name))
            return -1, "ERROR_SYMLINK_TARGET_DOESNOT_EXIST"

        # Ensure target is a regular file
        target_file = InodeNumber(target_inode)
        target_file.InodeNumberToInode(self.FileNameObject.RawBlocks)

        # Find available slot in directory data block
        fileentry_position = self.FileNameObject.FindAvailableFileEntry(dir)
        # ERROR_SYMLINK_DATA_BLOCK_NOT_AVAILABLE
        if fileentry_position == -1:
            logging.debug("ERROR_SYMLINK_DATA_BLOCK_NOT_AVAILABLE")
            return -1, "ERROR_SYMLINK_DATA_BLOCK_NOT_AVAILABLE"

        # Ensure binding not exists - if Lookup returns -1 it does not exist
        file_inode = self.FileNameObject.Lookup(name, dir)
        # ERROR_SYMLINK_ALREADY_EXISTS
        if file_inode != -1:
            logging.debug("ERROR_SYMLINK_ALREADY_EXISTS " + str(name))
            return -1, "ERROR_SYMLINK_ALREADY_EXISTS"

        # Find if there is an available inode
        inode_position = self.FileNameObject.FindAvailableInode()
        if inode_position == -1:
            logging.debug("ERROR_SYMLINK_INODE_NOT_AVAILABLE")
            return -1, "ERROR_SYMLINK_INODE_NOT_AVAILABLE"

        # ERROR_SYMLINK_TARGET_EXCEEDS_BLOCK_SIZE
        if len(bytearray(target, "utf-8")) > fsconfig.BLOCK_SIZE:
            logging.debug("ERROR_SYMLINK_TARGET_EXCEEDS_BLOCK_SIZE")
            return -1, "ERROR_SYMLINK_TARGET_EXCEEDS_BLOCK_SIZE"

        # get the absolute path name of target
        # self.FileNameObject.InsertFilenameInodeNumber(dir_inode, name, inode_position)

        # Create a softlink here
        inode_position = self.FileNameObject.FindAvailableInode()
        newfile_inode = InodeNumber(target_inode)
        newfile_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)
        newfile_inode.inode.refcnt += 1

        newsymlink_inode = InodeNumber(inode_position)
        newsymlink_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)
        newsymlink_inode.inode.type = fsconfig.INODE_TYPE_FILE
        newsymlink_inode.inode.size = len(target)
        newsymlink_inode.inode.refcnt = 1
        newsymlink_inode.inode.block_numbers[0] = self.FileNameObject.AllocateDataBlock()


        # Unlike DIRs, for FILES they are not allocated a block upon creatin; these are allocated on a Write()
        newsymlink_inode.StoreInode(self.FileNameObject.RawBlocks)
        self.FileOperationsObject.Write(inode_position, 0, bytearray(target, "utf-8"))
        newsymlink_inode.inode.type = fsconfig.INODE_TYPE_SYM
        newsymlink_inode.StoreInode(self.FileNameObject.RawBlocks)

        # Add to parent's (filename,inode) table
        self.FileNameObject.InsertFilenameInodeNumber(dir_inode, name, inode_position)


        # Update target inode
        #target_file.inode.refcnt += 1
        #target_file.StoreInode(self.FileNameObject.RawBlocks)

        # Update directory inode
        # refcnt incremented by one
        dir_inode.inode.refcnt += 1
        dir_inode.StoreInode(self.FileNameObject.RawBlocks)


        return inode_position, "SUCCESS"

