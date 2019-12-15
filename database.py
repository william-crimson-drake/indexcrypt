#Encoding UTF-8

import sqlite3

CONNECTIONTYPE = "indexes.db";#POSTGRESS:user=postgres port=5433 host=localhost dbname=indexes

def createBase():
    #create tables
    sql_session = sqlite3.connect(CONNECTIONTYPE);
    cursor = sql_session.cursor();

    cursor.execute("CREATE TABLE files (id integer PRIMARY KEY, name varchar, hash char(32) );")
    cursor.execute("CREATE TABLE blocks (id integer PRIMARY KEY, fulled boolean, void boolean);")
    cursor.execute('''CREATE TABLE indexes (
                        id_file integer REFERENCES files(id),
                        id_block integer REFERENCES blocks(id),
                        begin integer,
                        ended integer,
                        sequence integer,
                        PRIMARY KEY(id_file, id_block));
                        ''')
    cursor.execute('''CREATE TABLE emptiness (
                        id_block integer REFERENCES blocks(id),
                        number integer,
                        begin integer,
                        ended integer,
                        PRIMARY KEY(id_block, number));
                        ''')
    sql_session.commit()

    cursor.close()
    sql_session.close()


#-----------------------------------------------------add or enclose in base essence---------------------------------------
def add_file_in_base(file_name, file_hash):

    #create connection to database
    sql_session = sqlite3.connect(CONNECTIONTYPE)
    cursor = sql_session.cursor()

    #find if file already exist
    cursor.execute("SELECT id FROM files WHERE name=?", (file_name ,))
    id_list = cursor.fetchall()

    #if file already exist - notihing, else - add new file in base
    if len(id_list):
        print("Already exist file "+file_name+" in database.")
    else:
        #find max id of the blocks
        cursor.execute("SELECT MAX(id) FROM files;")
        t_max_id_list = cursor.fetchall()
        #agreagte function in SQLite always return cortage of number or none
        t_max_id = t_max_id_list[0][0]

        #set id for files
        file_id = None;
        if not(t_max_id is None):
            #files exists in database
            file_id = t_max_id+1;
        else:
            #it's first file in database
            file_id = 0;

        #insert new file
        cursor.execute("INSERT INTO files (id, name, hash) VALUES (?, ?, ?);", (file_id, file_name, file_hash, ))
        sql_session.commit()

    #close connection
    cursor.close()
    sql_session.close()

def remove_file_from_base(file_name):
    #create connection to database
    sql_session = sqlite3.connect(CONNECTIONTYPE)
    cursor = sql_session.cursor()

    cursor.execute("SELECT id FROM files WHERE name=?", (file_name,))
    id_list = cursor.fetchall()

    #if file already exist - notihing, else - add new file in base
    if not len(id_list):
        print("Don't exist file "+file_name+" in database.")
    else:
        idRemovedFile = id_list[0][0]
        #remove file
        cursor.execute("DELETE FROM files WHERE id=?;", (idRemovedFile,))
        sql_session.commit()

    #close connection
    cursor.close()
    sql_session.close()


def add_block_in_base():

    #create connection to database
    sql_session = sqlite3.connect(CONNECTIONTYPE)
    cursor = sql_session.cursor()

    #find max id of the blocks
    cursor.execute("SELECT MAX(id) FROM blocks;")
    t_max_id_list = cursor.fetchall()
    #agreagte function in SQLite always return cortage of number or none
    t_max_id = t_max_id_list[0][0]

    #set id for block
    block_id = None
    if not(t_max_id is None):
        #blocks exists in database
        block_id = t_max_id+1
    else:
        #it's first block in database
        block_id = 0

    #insert new block
    cursor.execute("INSERT INTO blocks (id, fulled, void) VALUES (?, ?, ?);", (block_id, False, False, ))
    sql_session.commit()

    #close connection
    cursor.close()
    sql_session.close()

    return block_id


#---------------------------------------add or enclose in derivative essence-----------------------------------------------------

def add_index_in_base(file_name, block_id, begin, end, sequence):

    #create connection to database
    sql_session = sqlite3.connect(CONNECTIONTYPE)
    cursor = sql_session.cursor()

    cursor.execute("SELECT id FROM files WHERE name=?", (file_name ,))
    file_id_list = cursor.fetchall()
    if not len(file_id_list):
        print("Don't exist file "+file_name+" in database.")
    else:
        file_id = file_id_list[0][0]

        cursor.execute("INSERT INTO indexes (id_file, id_block, begin, ended, sequence) VALUES (?, ?, ?, ?, ?);",
            (file_id, block_id, begin, end, sequence))
        sql_session.commit()
    #close connection
    cursor.close()
    sql_session.close()

    #set fulled and void properies for blocks
    set_properties_of_block()

def add_emptiness_in_base(block_id, begin, end):

    #create connection to database
    sql_session = sqlite3.connect(CONNECTIONTYPE)
    cursor = sql_session.cursor()

    #find all emptiness in specified block
    cursor.execute("SELECT id_block, number, begin, ended FROM emptiness WHERE id_block=?;", (block_id ,))
    empty_indexes_list = cursor.fetchall()

    if not len(empty_indexes_list):
        #if don't exist emptiness - insert empty index with number 0
        cursor.execute("INSERT INTO emptiness (id_block, begin, ended, number) VALUES (?, ?, ?, ?);", (block_id, begin, end, 0))
    else:
        #find indexes that touch specified emptiness
        link_empty_before_added = None
        link_empty_after_added = None
        for j in empty_indexes_list:
            if(j[3] == begin): # if some empty end where added empty begin
                link_empty_before_added = j
            if(j[2] == end): # if some empty begin where added empty end
                link_empty_after_added = j

        if (link_empty_before_added and link_empty_after_added):
            #if some empty indexes touch to left and right of the specified emptiness

            #set for left emptiness end of the right emptiness
            cursor.execute("UPDATE emptiness SET ended = ? WHERE id_block = ? AND number = ?;", (link_empty_after_added[3] ,link_empty_before_added[0], link_empty_before_added[1], ))

            #find max number in the block
            cursor.execute("SELECT MAX(number) FROM emptiness WHERE id_block=?;", (i[0] ,))
            t_max_number_list = cursor.fetchall()
            t_max_number = t_max_number_list[0][0]

            #remove right emptiness
            cursor.execute("DELETE FROM emptiness WHERE id_block = ? AND number = ?;", (link_empty_after_added[0], link_empty_after_added[1], ))
            #set for index wtih max number, number of removed index
            if(t_max_number != linkEmptyAfterRemoved[1]):
                cursor.execute("UPDATE emptiness SET number = ? WHERE id_block = ? AND number = ?;", (link_empty_after_added[1], link_empty_after_added[0], t_max_number, ))

        elif (link_empty_before_added and not link_empty_after_added):
            #if some empty indexes touch only to left of the specified emptiness
            #update left emptiness end
            cursor.execute("UPDATE emptiness SET ended = ? WHERE id_block = ? AND number = ?;", (end, link_empty_before_added[0], link_empty_before_added[1], ))
        elif (not link_empty_before_added and link_empty_after_added):
            #if some empty indexes touch only to right of the specified emptiness
            #update right emptiness begin
            cursor.execute("UPDATE emptiness SET begin = ? WHERE id_block = ? AND number = ?;", (begin, link_empty_after_added[0], link_empty_after_added[1], ))
        elif (not link_empty_before_added and not link_empty_after_added):
            #if don't exis empty index that touch specified emptiness
            #create new emptiness: find max number and set next number for created index
            cursor.execute("SELECT MAX(number) FROM emptiness WHERE id_block=?;", (block_id ,))
            t_max_number_list = cursor.fetchall()
            t_max_number = t_max_number_list[0][0]
            cursor.execute("INSERT INTO emptiness (id_block, number, begin, ended) VALUES (?, ?, ?, ?);", (block_id, (t_max_number+1), begin, end, ))

    #save changes of database
    sql_session.commit()

    #close connection
    cursor.close()
    sql_session.close()

    #set fulled and void properies for blocks
    set_properties_of_block()

#remove all indexs from base with specified block, file, or both
def remove_index_from_base(file_name = None, block_id = None):

    #create connection to database
    sql_session = sqlite3.connect(CONNECTIONTYPE);
    cursor = sql_session.cursor();

    #if file with this name exist, find its idFile
    file_id = None;
    if(file_name):
        cursor.execute("SELECT id FROM files WHERE name=?;", (file_name ,));
        file_id_list = cursor.fetchall();

        #if file don't exist in database - nothing, else conver list of id to id
        if len(file_id_list):
            file_id = file_id_list[0][0];
        else:
            print("Don't exist file "+file_name+" in database.");

    #remove all indexes using specified block, file_name(if file exist) or both
    #remove all indexes using block and file_name
    removed_indexes = None;
    if ( (file_id and file_name) and (block_id) ):
        #find all indexes that will be deleted
        cursor.execute("SELECT id_file, id_block, begin, ended FROM indexes WHERE id_file=? AND id_block=? ORDER BY sequence;",
            (file_id, block_id, ));
        removed_indexes = cursor.fetchall();

        #delete specified indexes
        cursor.execute("DELETE FROM indexes WHERE id_file=? AND id_block=?;", (file_id, block_id, ));
        sql_session.commit();
    #remove all indexes using file_name
    elif( (file_id and file_name) and not(block_id) ):
        #find all indexes that will be deleted
        cursor.execute("SELECT id_file, id_block, begin, ended FROM indexes WHERE id_file=? ORDER BY sequence;",
            (file_id, ));
        removed_indexes = cursor.fetchall();

        #delete specified indexes
        cursor.execute("DELETE FROM indexes WHERE id_file=?;", (file_id, ));
        sql_session.commit();
    #remove all indexes using block
    elif( not(file_name) and block_id ):
        #find all indexes that will be deleted
        cursor.execute("SELECT id_file, id_block, begin, ended FROM indexes WHERE id_block=? ORDER BY begin;", (block_id, ));
        removed_indexes = cursor.fetchall();

        #delete specified indexes
        cursor.execute("DELETE FROM indexes WHERE id_block=?;", (block_id, ));
        sql_session.commit();
    #if incorrect arguments
    else:
        removed_indexes = None;

    #close connection
    cursor.close();
    sql_session.close();

    #set fulled and void properies for blocks
    set_properties_of_block();

    return removed_indexes;

#remove all empty indexes from block, or specified block using its number
def remove_emptiness_from_base(block_id, number = None):

    #create connection to database
    sql_session = sqlite3.connect(CONNECTIONTYPE);
    cursor = sql_session.cursor();

    #remove specified empty index from block using number
    if (number):
        #find info about removed empty index
        cursor.execute("SELECT id_block, number, begin, ended FROM emptiness WHERE id_block=? AND number=? ORDER BY number;", (block_id, number));
        removed_indexes = cursor.fetchall();

        #if exist emty space with this number in block, remove it
        if not len(removed_indexes):
            print("Don't exist emptiness in block "+str(block_id)+" with number "+str(number)+" in database.");
        else:
            #find max number of emptiness from block (one number always exist)
            cursor.execute("SELECT MAX(number) FROM emptiness WHERE id_block=?;", (block_id,));
            maxNumberList = cursor.fetchall();
            maxNumber = maxNumberList[0][0];

            #delete specified index
            cursor.execute("DELETE FROM emptiness WHERE id_block=? AND number=?;", (block_id, number));
            sql_session.commit();

            #set removed number for empty index with max number
            cursor.execute("UPDATE emptiness SET number=? WHERE id_block=? AND number=?;", (number, block_id, maxNumber,));
            sql_session.commit();


    #remove all empty index from block
    else:
        #find info about removed empty indexes
        cursor.execute("SELECT id_block, number, begin, ended FROM emptiness WHERE id_block=? ORDER BY number;", (block_id,));
        removed_indexes = cursor.fetchall();

        #delete all indexes from block
        cursor.execute("DELETE FROM emptiness WHERE id_block=?;", (block_id, ));
        sql_session.commit();

    #close connection
    cursor.close();
    sql_session.close();

    #set fulled and void properies for blocks
    set_properties_of_block();

    return removed_indexes;

#---------------------------------------convert function-------------------------------------------------------------

def convert_from_index_to_empty(file_name):

    #list_removed_index - id_file, id_block, begin, end
    #remove all index with this file_name and create emptiness with this name
    list_removed_index = remove_index_from_base(file_name = file_name);

    if not len(list_removed_index):
        print("Can't convert index to empty: don't exist indexes of file "+file_name+". Possibly file was empty.");
    else:
        for removed_index in list_removed_index:
            add_emptiness_in_base(removed_index[1], removed_index[2], removed_index[3]);

def convret_from_empty_to_index(block_id, file_name, number, sequence):

    #list_removed_emptiness - id_file, number, begin, end
    #remove emtiness with this file_name and number, create with this name
    list_removed_emptiness = remove_emptiness_from_base(block_id, number = number);
    #must be only one elements
    if not len(list_removed_emptiness):
        print("Can't convert empty to index: don't exist emptines in the block "+str(block_id)+" with number "+str(number)+".");
    else:
        removed_empty = list_removed_emptiness[0];
        add_index_in_base(file_name, block_id, removed_empty[2], removed_empty[3], sequence);

#---------------------------------------set properties for entity-----------------------------------------------------
def set_properties_of_block():

    #create connection to database
    sql_session = sqlite3.connect(CONNECTIONTYPE);
    cursor = sql_session.cursor();

    #find all existing blocks
    cursor.execute("SELECT id FROM blocks ORDER BY id;");
    id_list = cursor.fetchall();

    for block_id_cortage in id_list:
        block_id = block_id_cortage[0];
        #find info about empty indexes in this block
        cursor.execute("SELECT id_block FROM emptiness WHERE id_block=? ORDER BY number;", [block_id]);
        empty_list = cursor.fetchall();

        #find info about empty indexes in this block
        cursor.execute("SELECT id_block FROM indexes WHERE id_block=?;", [block_id]);
        indexes_list = cursor.fetchall();

        #if don't exist emptiness for this block => this block is fulled
        if(not len(empty_list)):
            cursor.execute("UPDATE blocks SET fulled=? WHERE id=?;", [True, block_id]);
            sql_session.commit();
        else:
            cursor.execute("UPDATE blocks SET fulled=? WHERE id = ?;", [False, block_id]);
            sql_session.commit();

        #if don't exist indexes for this block => this block is empty
        if(not len(indexes_list)):
            cursor.execute("UPDATE blocks SET void=? WHERE id = ?;", [True, block_id]);
            sql_session.commit();
        else:
            cursor.execute("UPDATE blocks SET void=? WHERE id = ?;", [False, block_id]);
            sql_session.commit();

    #close connection
    cursor.close();
    sql_session.close();

def update_file_hash_in_base(file_name, file_hash):

    #create connection to database
    sql_session = sqlite3.connect(CONNECTIONTYPE);
    cursor = sql_session.cursor();

    #find if file don't exist
    cursor.execute("SELECT id FROM files WHERE name=?", (file_name ,));
    id_list = cursor.fetchall();

    if not len(id_list):
        print("Don't exist file "+file_name+" in database.");
    else:
        file_id = id_list[0][0];
        cursor.execute("UPDATE files SET hash = ? WHERE id = ?;", (file_hash, file_id, ));
        sql_session.commit();

    cursor.close();
    sql_session.close();

def check_file_hash(file_name):

    #connect to database
    sql_session = sqlite3.connect(CONNECTIONTYPE);
    cursor = sql_session.cursor();

    #sequel: find hash of file using  name of file
    cursor.execute("SELECT hash FROM files WHERE name=?;", (file_name, ));
    cortage_hash_list = cursor.fetchall();

    #close connect
    cursor.close();
    sql_session.close();

    #return hash(not list of hash), or none if file isn't exist
    file_hash = None;
    if len(cortage_hash_list):
        file_hash = cortage_hash_list[0][0];
    else:
        file_hash = None;

    return file_hash;

def table_of_files():
    files_list = [];

    #connect to database
    sql_session = sqlite3.connect(CONNECTIONTYPE);
    cursor = sql_session.cursor();

    cursor.execute("SELECT id, name, hash FROM files;");
    files_list = cursor.fetchall();

    #close connect
    cursor.close();
    sql_session.close();

    return files_list;

def table_of_indexes(file_name):

    #connect to database
    sql_session = sqlite3.connect(CONNECTIONTYPE);
    cursor = sql_session.cursor();

    cursor.execute("SELECT id FROM files WHERE name=?", (file_name ,));
    file_id_list = cursor.fetchall();

    indexes_list = None;
    if(not len(file_id_list) ):
        print("Don't exist file "+file_name+" in database.");
    else:
        file_id = file_id_list[0][0];

        cursor.execute("SELECT id_block, begin, ended FROM indexes WHERE id_file=? ORDER BY sequence;", (file_id, ));
        indexes_list = cursor.fetchall();

    #close connect
    cursor.close();
    sql_session.close();

    return indexes_list;

def table_of_blocks(full = None):

    #connect to database
    sql_session = sqlite3.connect(CONNECTIONTYPE);
    cursor = sql_session.cursor();

    indexes_list = None;
    if (full is None):
        cursor.execute("SELECT id, full, void FROM blocks;")
    else:
        cursor.execute("SELECT id, full, void FROM blocks WHERE full=?;", (full,));
    indexes_list = cursor.fetchall();

    #close connect
    cursor.close();
    sql_session.close();

    return indexes_list;

def table_of_emptiness():

    #connect to database
    sql_session = sqlite3.connect(CONNECTIONTYPE);
    cursor = sql_session.cursor();

    cursor.execute("SELECT id_block, number, begin, ended FROM emptiness;")
    indexes_list = cursor.fetchall();

    #close connect
    cursor.close();
    sql_session.close();

    return indexes_list;
