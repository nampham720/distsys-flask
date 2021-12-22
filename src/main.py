from datetime import time
import re
from flask import Flask, render_template, request
from flask.json import jsonify
import words_counting
from app_logic import CounterApp
import leader
import os

app = Flask(__name__)
counter_app = CounterApp(leader.BullyAlgorithm(3, 0.1))

@app.route("/", methods=['GET', 'POST'])
def index():
    if request.method == "POST":
        data = request.form["text"]
        mapperData = words_counting.mapper(data)
        frequency = words_counting.counting(mapperData)
        return render_template("results.html", result=frequency)
    return render_template("index.html")

@app.route("/assign", methods=['POST'])
def assign():
    file = request.form["file"]
    with open(file) as f:
        data = f.read()

    mapperData = words_counting.mapper(data)
    frequency = words_counting.counting(mapperData)
    return frequency

@app.route("/status", methods=['GET'])
def status():
    return "OK"

@app.route("/die", methods=['GET'])
def die():
    print(f"Shutting down {counter_app.get_my_ip()}")
    func = request.environ.get('werkzeug.server.shutdown')
    func()
    return "OK"

@app.route("/election", methods=["POST"])
def election():
    if counter_app.discovery_ready:
        counter_app.bully_algorithm.hold_election(
            counter_app.get_my_ip(),
            counter_app.other_nodes,
            False
        )
    return "OK"

@app.route("/victory", methods=["POST"])
def victory():
    counter_app.bully_algorithm.leader_ip = request.form["leader_ip"]
    return "OK"

if __name__ == "__main__":
    counter_app.start()
    app.run(debug=False, port=8000, host="0.0.0.0")
