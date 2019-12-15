#Encoding UTF-8

import argparse
import filework

def create_command_parser():
    parser = argparse.ArgumentParser()
    actionGroup = parser.add_mutually_exclusive_group(required=True)
    actionGroup.add_argument('-l', '--list', action='store_true', default=False)
    actionGroup.add_argument('-e', '--enclose', action='store_true', default=False)
    actionGroup.add_argument('-d', '--decompress', action='store_true', default=False)
    actionGroup.add_argument('-g', '--get', nargs='+')
    actionGroup.add_argument('-r', '--remove', nargs='+')

    return parser

if __name__ == "__main__":
    command_parser = create_command_parser()
    command_arg = command_parser.parse_args()


    if (command_arg.enclose):
        filework.assemble()
    elif (command_arg.decompress):
        filework.disassemble_all()
    elif (command_arg.get):
        for i in command_arg.get:
            filework.disassemble_file(i)
    elif (command_arg.remove):
        for i in command_arg.remove:
            filework.remove_file(i)
    elif (command_arg.list):
        filework.list_base_file()
