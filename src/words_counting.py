from itertools import count
import json 
import collections 
from functools import reduce
import multiprocess # for threading processing, if you guys want to. 


def mapper(data):
    '''
    Map each token to a NamedTuple 

    @param data: data received from user

    Return: a list of e.g. [Datapoint(token='123'), Datapoint(token='123')]
    '''
    datapoints = list()
    Datapoint = collections.namedtuple('Datapoint', ["token"])
    for d in data.split():
        datapoints.append(Datapoint(token=d.strip()))
    
    return datapoints

def reducer(acc, val):
    '''
    Reducer to perform Reduce function
    @param acc: accumulator,
    @param val: value
    '''
    try:
        acc[val] += 1
    except:
        acc[val] = 1
    return acc 


def counting(mapped_data):
    '''
    Counting the frequency of data

    @param mapped_data: data went through function mapper
    
    Return: {word: count}
    '''

    dd = collections.defaultdict(list)
    reduced = reduce(
        reducer, 
        mapped_data, 
        dd
    )
    results = dict()
    for i in reduced:
        results[i.token] = reduced[i]
    return results
