# Encoding UTF-8

import os
import hashlib


import database
import cloud
import crypt

WORKDIR = './work/';
BLOCKDIR = './.blocks/';
TEMPINFOFILE = '.info';
BLOCKSIZE = 10240;
CONNECTIONTYPE = "indexes.db";

def assemble():

    #create database if don't exist
    if not os.path.exists(CONNECTIONTYPE):
        database.create_base();


    #find all files in the work directory
    if not os.path.isdir(WORKDIR):
        print("Don't exist work dirctory '"+WORKDIR+"'.");
    else:
        objects_list = os.listdir(WORKDIR);
        files_list = [];

        #if object is directory or link, don't append it to list
        for i in objects_list:
            if not( os.path.isdir(WORKDIR+i) or os.path.islink(WORKDIR+i) ):
                files_list.append(i);# don't work for copy from NTFS

        if (not files_list):  #Empty directory without files
            print("Empty directory.");
        else:
            #create temp directory for blocks
            os.makedirs(BLOCKDIR, exist_ok=True);
            changed_blocks = [];
            for file_name in files_list: #examine all files in directory

                read_file_handler = open(WORKDIR+file_name, 'rb'); #open assembled file
                saved_file_hash = database.check_file_hash(file_name); #get old hash of file
                current_file_hash = hashlib.md5(read_file_handler.read()).hexdigest(); #get new hash of file

                #if file in base and hash updated, then move to emptiness indexes of this file
                if(saved_file_hash) and (saved_file_hash != current_file_hash):
                    database.convert_from_index_to_empty(file_name);

                #if file old and not updated then nothing, if file new or updated then assemble
                if (saved_file_hash) and (saved_file_hash == current_file_hash):
                        print("\033[0;33m" + "\t" + "\033[4;33m" + file_name+"\033[0m");
                else:
                    #if created new file, create file in database
                    #if file updated - update hash
                    if (not saved_file_hash):
                        database.add_file_in_base(file_name, current_file_hash);
                        print("\033[1;92m" + "NEW" + "\t" + "\033[4;92m" + file_name+"\033[0m");
                    else:
                        database.update_file_hash_in_base(file_name, current_file_hash);
                        print("\033[1;96m" + "UPDATED" + "\t" + "\033[4;96m" + file_name+"\033[0m");

                    #arguments for cycle
                    file_ended = False;
                    file_seek = 0;
                    sequence = 0;

                    #list of empty Indexes - list of cortage: [0] - block_id, [1] - number, [2] - begin, [3] - ended
                    empty_indexes = database.table_of_emptiness();

                    #insert file into emptiness, if they exist
                    if(empty_indexes):
                        for empty_index in empty_indexes:

                            #arguments, offset and sizes of emptiness
                            block_id = empty_index[0];
                            emptyNumber = empty_index[1];
                            begin_block_seek = empty_index[2];
                            end_block_seek = empty_index[3];
                            empty_size = end_block_seek-begin_block_seek;

                            #read info from file
                            read_file_handler.seek(file_seek);
                            part_file = read_file_handler.read(empty_size);

                            #if len of reading is 0 - file ended, else write readed part
                            if(len(part_file) == 0):
                                #file writed, end of cycle
                                file_ended = True;
                                break;
                            else:

                                #download using block if apsent
                                if not os.path.exists(BLOCKDIR+str(block_id)):
                                    cloud.download(BLOCKDIR, str(block_id));

                                #decrypt using block in temp file
                                crypt.decrypt(BLOCKDIR+str(block_id), BLOCKDIR+TEMPINFOFILE);

                                #open temp info file
                                write_block_handler = open(BLOCKDIR+TEMPINFOFILE, 'rb+');
                                write_block_handler.seek(begin_block_seek);

                                #write file in temp file
                                write_block_handler.write(part_file);
                                file_seek += len(part_file);
                                sequence += 1;
                                write_block_handler.close();

                                #encrypt changed block
                                crypt.encrypt(BLOCKDIR+TEMPINFOFILE, BLOCKDIR+str(block_id));
                                os.remove(BLOCKDIR+TEMPINFOFILE);

                                #mark encrypted block as changed
                                changed_blocks.append(block_id);

                                if(len(part_file) == empty_size):
                                    #if reading part of file euqel emptiness
                                    database.convret_from_empty_to_index(block_id, file_name, emptyNumber, sequence);
                                else:
                                    #if file ended and part of file less then emptiness
                                    database.remove_emptiness_from_base(block_id, number = emptyNumber);
                                    database.add_index_in_base(file_name, block_id, begin_block_seek, begin_block_seek+len(part_file), sequence);
                                    database.add_emptiness_in_base(block_id, begin_block_seek+len(part_file), end_block_seek)

                                    #file writed, end of cycle
                                    file_ended = True;# for while cycle(full blocks)
                                    break;# for i in cycle(emptiness in blocks)
                    #create new blocks, because emptiness don't exist
                    while (not file_ended):

                        #read info from file
                        read_file_handler.seek(file_seek);
                        part_file = read_file_handler.read(BLOCKSIZE);

                        #calculate properties of index
                        begin_index_seek = 0;
                        end_index_seek = len(part_file);

                        #if len of reading is 0 - file ended, else write readed part
                        if(len(part_file) == 0):
                            #file writed, end of cycle
                            file_ended = True;
                        else:
                            write_block_handler = open(BLOCKDIR+TEMPINFOFILE, 'wb');
                            block_id = database.addBlockInBase();

                            if(len(part_file) == BLOCKSIZE):
                                #if reading part of file euqel emptiness, add index in database
                                database.add_index_in_base(file_name, block_id, begin_index_seek, end_index_seek, sequence);
                            else:
                                #if file ended and part of file less then add index and emptiness in database
                                database.add_index_in_base(file_name, block_id, begin_index_seek, end_index_seek, sequence);
                                database.add_emptiness_in_base(block_id, end_index_seek, BLOCKSIZE);

                                #file writed, end of cycle
                                file_ended = True;

                            write_block_handler.write(part_file);
                            file_seek += len(part_file);
                            sequence += 1;

                            #add zero byte in the end of block
                            #!bad suggestion
                            zero_size = BLOCKSIZE - len(part_file);
                            while (zero_size > 0):
                                write_block_handler.write(bytes([0]));
                                zero_size -= 1;

                            write_block_handler.close();

                            #encrypt changed block
                            crypt.encrypt(BLOCKDIR+TEMPINFOFILE, BLOCKDIR+str(block_id));
                            os.remove(BLOCKDIR+TEMPINFOFILE);

                            #mark encrypted block as changed
                            changed_blocks.append(block_id);

                read_file_handler.close();

            #remove duplicate form list
            changed_blocks = list(set(changed_blocks))

            #upload marked blocks and remove blocks files
            if(len(changed_blocks)):
                print("Uploading");
                i = 0;
                for block_id in changed_blocks:
                    cloud.upload(BLOCKDIR, str(block_id));
                    os.remove(BLOCKDIR+str(block_id));
                    i += 1;
                    print( str(i)+"/"+str(len(changed_blocks)) );

            #remove block dir (it is empty)
            #os.rmdir(BLOCKDIR);


def disassemble_file(file_name):

    #error if don't exist database
    if not os.path.exists(CONNECTIONTYPE):
        print("Don't exist database");
    else:
        #checkHash return None if file don't exist, and hash if exist
        if(database.check_file_hash(file_name) is None):
            print("File '"+file_name+"' don't exist in database.");
        else:
            #create temp directory for blocks
            os.makedirs(BLOCKDIR, exist_ok=True);

            #find all index of this file
            indexes_table = database.table_of_indexes(file_name);
            #if don't exist indexes of this file - this is empty file
            if(not indexes_table):
                write_file_handler = open(WORKDIR+file_name, 'wb');
                write_file_handler.close();
            else:

                write_file_handler = open(WORKDIR+file_name, 'wb');

                for index_string in indexes_table:
                    block = [];

                    #save all info about index
                    block_number = index_string[0];
                    index_begin = index_string[1];
                    index_end = index_string[2];
                    index_size = index_end-index_begin;

                    #download using block
                    cloud.download(BLOCKDIR, str(block_number));

                    #decrypt using block in temp file
                    crypt.decrypt(BLOCKDIR+str(block_number), BLOCKDIR+TEMPINFOFILE);

                    #read info
                    read_file_handler = open(BLOCKDIR+TEMPINFOFILE, 'rb');
                    read_file_handler.seek(index_begin);
                    block.extend(read_file_handler.read(index_size));
                    read_file_handler.close();

                    #read info in specified file
                    write_file_handler.write(bytes(block));

                    #remove temp file for info
                    os.remove(BLOCKDIR+TEMPINFOFILE);

                    #remove block files
                    os.remove(BLOCKDIR+str(block_number));

                write_file_handler.close();
            print("\033[0;94m" + "DOWNLOAD" + "\t" + "\033[4;94m" + file_name + "\033[0m");
            #remove block dir (it is empty)
            #os.rmdir(BLOCKDIR);

def disassemble_all():#rewrite as call of decryptFile

    #error if don't exist database
    if not os.path.exists(CONNECTIONTYPE):
        print("Don't exist database");
    else:
        #find all files in database
        files_list = database.table_of_files();

        #for every files
        for fileRow in files_list:
            #fileRow - [0]-id, [1]-name, [2]-hash
            file_name = fileRow[1];
            disassemble_file(file_name);

def remove_file(file_name):

    #error if don't exist database
    if not os.path.exists(CONNECTIONTYPE):
        print("Don't exist database");
    else:
        #checkHash return None if file don't exist, and hash if exist
        if(database.check_file_hash(file_name) is None):
            print("File '"+file_name+"' don't exist in database.");
        else:
            database.convert_from_index_to_empty(file_name);
            database.remove_file_from_base(file_name);
            print("\033[0;94m" + "DELETE" + "\t" + "\033[4;94m" + file_name + "\033[0m");

def list_base_file():

    #error if don't exist database
    if not os.path.exists(CONNECTIONTYPE):
        print("Don't exist database");
    else:
        files_table = database.table_of_files();
        for i_file in files_table:
            file_name = i_file[1];
            if os.path.exists(WORKDIR+file_name) and (not os.path.isdir(WORKDIR+file_name)) and (not os.path.islink(WORKDIR+file_name)) :
                print("\033[0;92m" + "EXIST" + "\t" + "\033[4;92m" + file_name + "\033[0m");
            else:
                print("\033[0;91m" + "ABSENT" + "\t" + "\033[4;91m" + file_name + "\033[0m");
