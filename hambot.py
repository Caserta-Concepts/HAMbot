import ConfigParser
import os
import sys
import importlib
import logging
import argparse
from flask import Flask, Response
from flask import request


#------------------------------------------------------------------------
#declare application
app = Flask(__name__)
app.debug = True


#------------------------------------------------------------------------
#utils
def log(message):
    logging.info(message)
    print(message)

#------------------------------------------------------------------------
#other stuff
def getTests(job_id):
    #imagine a db connection here
    return ['test1','test2']

#------------------------------------------------------------------------
#index
@app.route('/')
def index():
    return 'HAMbot 0.0'

#------------------------------------------------------------------------
#check quality
@app.route('/check_dq/<string:job_id>')

def check_dq (job_id):
    results=[]

    script_path = os.path.dirname(os.path.realpath(__file__)) + '/tests'
    sys.path.insert(1,script_path)

    test_list = getTests(job_id)

    for test in test_list:
        module = __import__(test)
        result = module.test()
        results.append(result)

        #imagine we trap the assertions and compare to thresholds..
        log(result)

    return ' | '.join(results)

#------------------------------------------------------------------------
if __name__ == '__main__':
    app.run()