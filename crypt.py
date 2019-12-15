#Encoding UTF-8

KEY = ""

# read info block, encrypt it and save encrypt block
def encrypt(in_file, out_file):
    read_block_handler = open(in_file, 'rb');
    write_block_handler = open(out_file, 'wb');

    block_info = read_block_handler.read();
    # NOTE
    # ENCRYPT FROM OpenSSL
    write_block_handler.write(block_info);

    read_block_handler.close();
    write_block_handler.close();

# read encrypt block, dencrypt it and save info block
def decrypt(inFile, out_file):
    read_block_handler = open(inFile, 'rb');
    write_block_handler = open(out_file, 'wb');

    block_info = read_block_handler.read();
    # NOTE
    # DECRYPT FROM OpenSSL
    write_block_handler.write(block_info);

    read_block_handler.close();
    write_block_handler.close();
