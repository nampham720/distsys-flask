FROM python:3.9

WORKDIR /distsys-app 

COPY requirements.txt . 

RUN pip install -r requirements.txt 

COPY ./src ./src 

RUN mkdir shared

CMD ["python", "-u", "./src/main.py"]