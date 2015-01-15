import ConfigParser
import os
import sys
import logging
import argparse
import datetime
from flask import Flask

#------------------------------------------------------------------------
#declare application
app = Flask(__name__)
app.debug = True


#------------------------------------------------------------------------
#utils

#logging wrapper:
log_name = datetime.datetime.now().strftime("hambot-%Y%m%d-%H%M%S.log")
logging.basicConfig(filename=log_name,level=logging.DEBUG, format='%(asctime)s %(message)s',filmode='w')

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
    return 'HAMbot 2.0'

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
#check quality
@app.route('/run_tests/<string:testModule>')
def run_tests(testModule):

    #Dynamically load test module
    testModule = __import__(testModule)

    results=[]

    tset = {}
    tset = testModule.get_test_set()

    ### Sort Test Set by Key(Test) and run one by one
    for test in sorted(tset):
        #Set the current test in test module.  Used for updating test Dictionary within test module
        testModule.set_current_test(tset[test])

        #Run the test and get results
        tset[test]['result'] = tset[test]['function'](**tset[test]['params'])

        #Set Pass/Fail Status
        tset[test]['status'] = get_test_status(tset[test]['compType'], tset[test]['result'], tset[test]['upperLim'], tset[test]['lowerLim'])

        #Create Report Record
        record = get_report_record(**tset[test])
        results.append(record)

        print tset[test]

        #imagine we trap the assertions and compare to thresholds..
        #log(result)

    return ' | '.join(results)

#------------------------------------------------------------------------
def get_test_status(comp, result, upperlim, lowerlim):
    status = "FAILED"
    if comp.upper() == "EQ":
        if result == upperlim or result == lowerlim:
            status = "PASSED"

    elif comp.upper() == "NE":
        if result != upperlim and result != lowerlim:
            status = "PASSED"

    elif comp.upper() == "GT":
        if result > upperlim:
            status = "PASSED"

    elif comp.upper() == "LT":
        if result < lowerlim:
            status = "PASSED"

    elif comp.upper() == "GELE":
        if result <= upperlim and result >= lowerlim:
            status = "PASSED"

    elif comp.upper() == "GTE":
        if result >= upperlim:
            status = "PASSED"

    elif comp.upper() == "LTE":
        if result <= lowerlim:
            status = "PASSED"
    else:
        print "Invalid comparision type"

    return status

#------------------------------------------------------------------------
def get_report_record(name, description, function, params, result, upperLim, lowerLim, compType, status):

    name = add_padding(name, 6)
    description = add_padding(description, 40)
    result = add_padding(str(result), 10)
    upperLim = add_padding(str(upperLim), 8)
    lowerLim = add_padding(str(lowerLim), 8)
    compType = add_padding(compType, 6)
    status = add_padding(status, 8)

    record = name + '\t' + description + '\t' + result + '\t' + compType + '\t' + lowerLim + '\t' + upperLim + '\t' + status + '\n'

    return record

#------------------------------------------------------------------------
def add_padding(str, length):

    if str != "" and length > 0:
        if len(str) > length:
            str = str[:length -1]
        else:
            str = "{0:{1}}".format(str, length)
    return str

#------------------------------------------------------------------------
#if __name__ == '__main__':
#    app.run()

run_tests('s3_session_count_check')