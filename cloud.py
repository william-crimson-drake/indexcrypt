#Encoding UTF-8

import dropbox

CLOUDPATH = "/"
TOKEN = ""

def upload(local_path, file_name):

    dbox = dropbox.Dropbox(TOKEN)

    try:
        dbox.users_get_current_account()
    except dropbox.exceptions.DropboxException:
        print("Can't connect to DropBox '"+file_name+"'.")
    else:

        read_file_handler = open(local_path+file_name, 'rb')

        try:
            dbox.files_upload(read_file_handler.read(), CLOUDPATH+file_name, mode=dropbox.files.WriteMode('overwrite'))
        except dropbox.exceptions.DropboxException:
            print("Can't upload '"+file_name+"'.")
        finally:
            read_file_handler.close()

def download(local_path, file_name):

    dbox = dropbox.Dropbox(TOKEN)

    try:
        dbox.users_get_current_account()
    except dropbox.exceptions.DropboxException:
        print("Can't connect to DropBox '"+file_name+"'.")
    else:
        try:
            dbox.files_download_to_file(local_path+file_name, CLOUDPATH+file_name)
        except dropbox.exceptions.ApiError:
            print("Can't download '"+file_name+"'.")
