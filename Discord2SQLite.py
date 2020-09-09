#!/usr/bin/python
import os
import csv
import sqlite3
import re
import sys
import requests
import random
from tqdm import tqdm

conn = sqlite3.connect(r"Discord.sqlite3")
conn.execute("""CREATE TABLE IF NOT EXISTS chat(
    id INTEGER PRIMARY KEY NOT NULL,
    Author TEXT NOT NULL,
    Recipient TEXT NOT NULL,
    Date TEXT NOT NULL,
    Content TEXT,
    Attachments TEXT,
    Reactions TEXT
)""")
c = conn.cursor()

if !os.path.isdir("attachments"):
    os.mkdir("attachments")
if !os.path.isdir("logs"):
    os.mkdir("logs")

def getMessageType(name):
    if re.search("DM .*$", name):
        return("DM")
    else:
        return("Server")

def getDMNames(data):
    names = []
    row = 0
    while len(names) < 2:
        name = data[row][0]
        if name not in names and name != "Author":
            names.append(name)
        row += 1
    return(names)

# Read data from file and return it as a list of tuples
def readData(filename):
    data = []
    if os.path.isfile(filename):
        with open(filename, encoding="utf8") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in tqdm(reader):
                # Sometimes the list will have one too many rows. This makes sure it only has 5. It also cuts out
                # the 'AuthorID' row, because I don't want it.
                t = row[1:6]
                #print(t)
                if t != ["Author", "Date", "Content", "Attachments", "Reactions"]:
                    if re.search("http[s]?://cdn.discordapp.com.attachments/.*", t[3]):
                        # If there's already a file with the given name, add some random characters to the end
                        fname = t[3].split(r"/")[-1].splitlines()[0]
                        if os.path.isfile(os.path.join(r"attachments", fname)):
                            ext = fname.split(".")[-1]
                            h = ["0","1","2","3","4","5","6","7","8","9","A","B","C","D","E","F"]
                            fname = ''.join(random.choice(h) for i in range(10))
                            fname += f".{ext}"
                        with open(f"attachments/{fname}", 'wb') as dlf:
                            dlf.write(requests.get(t[3]).content)
                        t[3] = fname
                    if re.search("http[s]?://cdn.discordapp.com.attachments/.*", t[2]):
                        fname = t[2].split(r"/")[-1].splitlines()[0]
                        if os.path.isfile(os.path.join(r"attachments", fname)):
                            ext = fname.split(".")[-1]
                            h = ["0","1","2","3","4","5","6","7","8","9","A","B","C","D","E","F"]
                            fname = ''.join(random.choice(h) for i in range(10))
                            fname += f".{ext}"
                        with open(f"attachments/{fname}", 'wb') as dlf:
                            dlf.write(requests.get(t[2]).content)
                        t[2] = fname
                    data.append(tuple(t))
        return(data)
    else:
        print("File Not Found.")
        sys.exit(1)

def importDM(names, data):
    c.execute("SELECT Author, Recipient, Date, Content, Attachments, Reactions FROM Chat")
    cdata = c.fetchall()
    count = 0
    skipped = 0

    for t in data:
        # Converts the tuple to a list so it's mutable and then adds the recipient field
        l = list(t)
        if l[0] == names[0]:
            l.insert(1, names[1])
        else:
            l.insert(1, names[0])
        t = tuple(l)
        if t in cdata:
            skipped += 1
        else:
            c.execute('INSERT INTO Chat (Author, Recipient, Date, Content, Attachments, Reactions) VALUES (?,?,?,?,?,?)', t)
            count += 1
    print(f"Inserted {count} rows.")
    print(f"Skipped {skipped} rows")
    conn.commit()

def importServer(sPath):
    c.execute("SELECT * FROM Chat")
    cdata = c.fetchall()
    for f in os.listdir(sPath):
        fPath = fname + "/" + f
        data = readData(fPath)
        count = 0
        skipped = 0

        for t in tdqm(data):
            l = list(t)
            l.insert(1, f.split('.')[0])
            t = tuple(l)
            if t in cdata:
                skipped += 1
            else:
                c.execute('INSERT INTO Chat (Author, Recipient, Date, Content, Attachments, Reactions) VALUES (?,?,?,?,?,?)', t)
                count += 1
    print(f"Inserted {count} rows.")
    print(f"Skipped {skipped} rows.")
    conn.commit()

fname = input("File Name: ")
mtype = getMessageType(fname)
if mtype == "DM":
    d = readData(fname)
    importDM(getDMNames(d), d)
elif mtype == "Server":
    importServer(fname)
else:
    print("Invalid File Name!")
    sys.exit(1)
