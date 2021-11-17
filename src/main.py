import re
from flask import Flask, render_template, request
from flask.json import jsonify
import words_counting
# import uvicorn


app = Flask(__name__)

@app.route("/", methods=['GET', 'POST'])
def index():
    if request.method == "POST":
        data = request.form["text"]
        mapperData = words_counting.mapper(data)
        frequency = words_counting.counting(mapperData)
        return render_template("results.html", result=frequency)
    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True, port=8000, host="0.0.0.0")

