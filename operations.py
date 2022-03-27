from pymongo import MongoClient

port_number = int(input("Enter the port number: "))
client = MongoClient("mongodb://localhost:"+str(port_number)+"/")
db = client["291db"]


def search_titles():
    pass


def search_genres():
    pass


def search_cast():
    pass


def add_movie():
    pass


def add_cast():
    pass