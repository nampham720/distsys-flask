from datetime import time
import re
from flask import Flask, render_template, request
from flask.json import jsonify
import words_counting
from app_logic import CounterApp

app = Flask(__name__)
counter_app = CounterApp()

@app.route("/", methods=['GET', 'POST'])
def index():
    if request.method == "POST":
        data = request.form["text"]
        mapperData = words_counting.mapper(data)
        frequency = words_counting.counting(mapperData)
        return render_template("results.html", result=frequency)
    return render_template("index.html")

@app.route("/status", methods=['GET'])
def status():
    return "OK"

@app.route("/die", methods=['GET'])
def die():
    counter_app.quit = True

if __name__ == "__main__":
    counter_app.start()
    app.run(debug=False, port=8000, host="0.0.0.0")

