import connection as c 
from flask import Flask, jsonify
from markupsafe import escape
from bson import ObjectId
import json

app = Flask(__name__)


db = c.get_db_connection("200.1.17.171",21100,'wildsense','Wildsense',"pythonuser","pythonuser",27019,"db-cloud")

@app.get("/buzo/all")
def get_all_buzos():
    result = db["Buzo"].find({})
    output = []
    
    for document in result:
        document["_id"] = str(document["_id"])
        output.append(document)

    return jsonify(output)

@app.get("/buzo/<id>")
def find_buzo(id):
    result = db["Buzo"].find({"_id":{"$eq": ObjectId(str(id))}})
   
    return f'{result[0]}'

