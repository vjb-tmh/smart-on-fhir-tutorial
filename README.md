Testing Cerner's SMART on FHIR API
----------------------------------

License
------------------------------
Copyright 2017 Cerner Innovation, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Tested using Python v2.7.14

To run the program:
   (1) Extract the tarball to a directory of your choice.
   (2) Execute the command 'python prog.py' via a terminal in the directory you extracted the tarball to.

The program will parse the data files, spin up a work_queue thread to insert jobs into the queue, and spin up three data_worker threads to retrieve jobs from the queue and insert data into the database.

After all jobs are retrieved from the queue, the program will continue to check for modifications to the data files.  If one of the files is modified, it will repeat the process described above.

Example:

$ python prog.py

Agent data inserted into database.
Listing data inserted into database.
Office data inserted into database.

<<< Waiting for new data. >>>



#!/usr/bin/python

import os
import sqlite3
import json
import csv
import xml.etree.ElementTree as ET
import time
import Queue
import threading

agent_data = []
listing_data = []
office_data = []
json_mtime = None
xml_mtime = None
acsv_mtime = None
lcsv_mtime = None
ocsv_mtime = None
num_data_workers = 3
con = sqlite3.connect(r'data/homes.com/homes.db', check_same_thread=False)

def get_agent_data():
    """
    Aggregate agent data
    @return: list of agents
    """
    data = []
    global acsv_mtime
    if(os.path.isfile("data/mls003/agents.csv") and
        acsv_mtime != os.path.getmtime("data/mls003/agents.csv")):
        acsv_mtime = os.path.getmtime("data/mls003/agents.csv")
        data = data + parse_agent_csv()

    return data

def get_listing_data():
    """
    Aggregate listing data
    @return: list of listings
    """
    data = []
    global xml_mtime
    global json_mtime
    global lcsv_mtime
    if(os.path.isfile("data/mls001/data.xml") and
        xml_mtime != os.path.getmtime("data/mls001/data.xml")):
        xml_mtime = os.path.getmtime("data/mls001/data.xml")
        data = parse_xml_data()

    if(os.path.isfile("data/mls002/feed.json") and
        json_mtime != os.path.getmtime("data/mls002/feed.json")):
        json_mtime = os.path.getmtime("data/mls002/feed.json")
        data = data + parse_json_data()

    if(os.path.isfile("data/mls003/listings.csv") and
        lcsv_mtime != os.path.getmtime("data/mls003/listings.csv")):
        lcsv_mtime = os.path.getmtime("data/mls003/listings.csv")
        data = data + parse_listing_csv()

    return data

def get_office_data():
    """
    Aggregate office data
    @return: list of offices
    """
    data = []
    global ocsv_mtime
    if(os.path.isfile("data/mls003/offices.csv") and
        ocsv_mtime != os.path.getmtime("data/mls003/offices.csv")):
        ocsv_mtime = os.path.getmtime("data/mls003/offices.csv")
        data = parse_office_csv()

    return data

def parse_json_data():
    """
    Parse json data
    listings = list of dicts
    listings[i] = dict for single listing
    @return: list of listings
    """
    listings = json.load(open('data/mls002/feed.json'))

    # extract listings
    data = []
    single_listing = []
    for listing in listings:
        single_listing = []
        single_listing.append(None)
        single_listing.append(listing['street_address'])
        single_listing.append(listing['city'])
        single_listing.append(listing['state'])
        single_listing.append(listing['zip'])
        single_listing.append(listing['mls_number'])
        single_listing.append(listing['price'])
        single_listing.append(listing['status'])
        single_listing.append(listing['type'])
        single_listing.append(listing['description'])
        single_listing.append(listing['agent_code'])
        single_listing.append(listing['office_code'])
        data.append(list(single_listing))

    return data

def parse_xml_data():
    """
    Parse xml data
    @return: list of listings
    """
    # parse xml tree
    tree = ET.parse('data/mls001/data.xml')

    # get the root of the tree, e.g. listings
    root = tree.getroot()

    # find each listing in the tree
    # extract listings
    data = []
    single_listing = []
    for listing in root.findall('listing'):
        single_listing = []
        single_listing.append(None)
        single_listing.append(listing.find('address').find('street').text)
        single_listing.append(listing.find('address').find('city').text)
        single_listing.append(listing.find('address').find('state').text)
        single_listing.append(listing.find('address').find('zip').text)
        single_listing.append(listing.find('mls_number').text)
        single_listing.append(listing.find('price').text)
        single_listing.append(listing.find('status').text)
        single_listing.append(listing.find('type').text)
        single_listing.append(listing.find('description').text)
        single_listing.append(listing.find('agent').find('code').text)
        single_listing.append(listing.find('broker').find('code').text)
        data.append(list(single_listing))

    return data

def parse_office_csv():
    """
    Parse office csv data
    @return: list of offices
    """
    data = []
    with open('data/mls003/offices.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile,delimiter=',')
        cnt = 0
        for row in reader:
            if(cnt == 0):
                cnt += 1
                continue
            row.insert(0,None)
            data.append(list(row))

    return data

def parse_listing_csv():
    """
    Parse listing csv data
    @return: list of listings
    """
    data = []
    with open('data/mls003/listings.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile,delimiter=',')
        cnt = 0
        for row in reader:
            if(cnt == 0):
                cnt += 1
                continue
            tmp = row.pop(0)
            row.insert(4,tmp)
            tmp = row.pop(-1)
            row.insert(8,tmp)
            row.insert(0,None)
            data.append(list(row))

    return data

def parse_agent_csv():
    """
    Parse agent csv data
    @return: list of agents
    """
    # get agents csv data
    data = []
    with open('data/mls003/agents.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile,delimiter=',')
        cnt = 0
        for row in reader:
            if(cnt == 0):
                cnt += 1
                continue
            row.pop(2) # remove data for second column
            row.insert(0,None)
            data.append(list(row))

    return data

def parse_worker():
    """
    Worker to parse data files
    @return: true if data exists, false otherwise
    """
    global agent_data
    global listing_data
    global office_data
    agent_data = get_agent_data()
    listing_data = get_listing_data()
    office_data = get_office_data()
    if(agent_data):
        agent_data.insert(0,'agent')
    if(listing_data):
        listing_data.insert(0,'listing')
    if(office_data):
        office_data.insert(0,'office')
    if(agent_data or office_data or listing_data):
        return True
    else:
        return False

def work_queue(q):
    """
    Work queue to hold jobs
    @param q: Queue object
    """
    global agent_data
    global listing_data
    global office_data
    if(agent_data):
        q.put(agent_data)
        time.sleep(0.1)
    if(listing_data):
        q.put(listing_data)
        time.sleep(0.1)
    if(office_data):
        q.put(office_data)
        time.sleep(0.1)
    q.join()

def data_worker(q):
    """
    Extract jobs from work queue
    Insert data into database
    @param q: Queue object
    """
    data = q.get()
    curs = con.cursor()
    if(data[0] == 'agent'):
        data.pop(0)
        time.sleep(0.1)
        query = "insert into agents values (?,?,?,?,?,?,?)"
        for agent in data:
            curs.execute(query,tuple(agent))
        print 'Agent data inserted into database.'
        con.commit()
    elif(data[0] == 'office'):
        data.pop(0)
        time.sleep(0.1)
        query = "insert into offices values (?,?,?,?,?,?,?)"
        for office in data:
            curs.execute(query,tuple(office))
        print 'Office data inserted into database.'
        con.commit()
    elif(data[0] == 'listing'):
        data.pop(0)
        time.sleep(0.1)
        query = "insert into listings values (?,?,?,?,?,?,?,?,?,?,?,?)"
        for listing in data:
            curs.execute(query,tuple(listing))
        print 'Listing data inserted into database.'
        con.commit()
    q.task_done()

if __name__ == "__main__":
    """
    Test for new data to parse
    Parse data and spin up work queue and data worker threads
    Produce and consume jobs
    """
    # set up queue
    q = Queue.Queue(maxsize = 3)

    waiting = False
    while True:
        if(parse_worker()):
            waiting = False

            threads = []
            # spin up data worker threads
            for i in range(num_data_workers):
                t = threading.Thread(target=data_worker,args=(q,))
                threads.append(t)
                t.start()

            # spin up work queue thread
            t = threading.Thread(target=work_queue,args=(q,))
            threads.append(t)
            t.start()

            # kill threads on keyboard interrupt
            while len(threads) > 0:
                try:
                    threads = [t.join(1) for t in threads
                                if t is not None and t.isAlive()]
                except KeyboardInterrupt:
                    for t in threads:
                        t.kill_received = True

            q.join()
            time.sleep(1)
        else:
            if(waiting == False):
                waiting = True
                print '\n<<< Waiting for new data. >>>'
