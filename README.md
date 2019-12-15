# README

## Description

This utilite upload and download encrypted user's files on untrusted cloude storage ```DropBox```.
It is written in ```Python 3``` and  uses module for working with ```SQLite``` database.

The ```DropBox``` storage contains only a fixed-length enumerated encrypted block-files.
Unlike file-by-file encryption method, in this case it is difficult to determine where every encrypted file is stored.
Unlike file archive encryption, in this case there is no needing to download and upload entire archive when changed only one file.

Every block-file from storage contains parts of different user's files.
Special indexes point correspondence between parts of the user's files and their offsets in the block-files.
All indexes is stored in the ```SQLite``` database file.

When new file is created in the working directory or existing file is changed,
the utility indexes these files and store them in free places in the block-files,
then sends changed block-files to the storage.

> NOTE: Currently encryption function is temporarily replaced by a dummy function.

## Content of repository

- ```README.md``` - this readme file;
- ```.gitignore``` - wildcard of files, that will not save in repository;
- ```indexcrypt.py``` - file of utility with main function and processing of CLI args;
- ```filework.py``` - module of processing work directory with user's files;
- ```database.py``` - module for working with indexes in ```SQLite``` database file;
- ```crypt.py``` - module of encrypting and decrypting block-files;
- ```cloud.py``` - module of file downloading and uploading to ```DropBox```.

## Dependencies
There is dependency ```Python 3``` package for server:

 - ```dropbox```

Module ```sqlite3``` is one of the standard ```python3``` modules.

## Arguments of program

- ```-h, --help``` - print help;
- ```-l, --list``` - print list of storaged files;
- ```-e, --enclose``` - read work dir and update indexes, then send new and changed files to the storage;
- ```-d, --decompress``` - obtain and decrypt all files from storage;
- ```-g/--get```=```files ...``` obtain and decrypt specified files from storage;
- ```-r/--remove```=```files ...``` - remove specified file from index database (not from work dir).

Remove file ```indexes.db``` for restart.

## Example of using:

```
python3 indexcrypt.py -e
python3 indexcrypt.py --get file1.txt file2.jpg
```
