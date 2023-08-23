# small script to push to webpage via ftp

import ftplib


def push_with_ftp():
    FTP_HOST = "" # your websit
    FTP_USER = "" # your login
    FTP_PASS = "" # your password

    # connect to the FTP server
    ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
    # force UTF-8 encoding
    ftp.encoding = "utf-8"

    ftp.cwd("html/projects")

    # local file name you want to upload
    filenames = [
        "output.png",
        "new_org_table.html",
        "projects.html",
        "report_header.html",
    ]

    for filename in filenames:
        with open(filename, "rb") as file:
            # use FTP's STOR command to upload the file
            ftp.storbinary(f"STOR {filename}", file)

    # list current files & directories

    ftp.dir()
    print("\n")
    ftp.quit()


if __name__ == "__main__":
    push_with_ftp()
