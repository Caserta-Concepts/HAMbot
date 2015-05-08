import ConfigParser
import os
import sys
import logging
import traceback
import argparse
from  datetime import datetime
import run_multi
import hambot_test_class as test_Module
#from app import app
from flask import Flask, render_template, flash, redirect


#define err messages
# ------------------------------------------------------------------------
err_config = 'The Config File: \'{0}\' is not recognized by python.\n\n'\
             'Please verify the file name and path.  All config files need to reside in the HAMbot/cfg directory'

# ------------------------------------------------------------------------
#declare application
app = Flask(__name__, static_url_path = "/static", static_folder = "static")
app.debug = True

# ------------------------------------------------------------------------
@app.route('/index')        #http://localhost:5000/index
def index():
     return 'HAMbot 2.0'

#run data quality tests -- http://localhost:5000/Theorem/Simul_Data_Test/run
@app.route('/<string:config>/<string:testSet>/run', endpoint='run')
def run(config, testSet):
    try:
        config_path = "{0}/cfg/{1}.cfg".format(sys.path[0], config)
        #First verify config file exists
        if os.path.isfile(config_path):
            job = test_Module.DqTesting(config_path)
            logo_url = job.logo_url
            test_set = job.get_test_set(testSet)

            #Verify Test Set is valid
            if test_set is None:
                valid_test_sets = job.get_all_test_sets()
                for my_set in valid_test_sets:
                    my_set['url'] = '/{0}/{1}/run'.format(config, my_set['test_set_name'])

                return render_template("test_set_invalid.html",
                               title=config,
                               logo = logo_url,
                               test_set = testSet,
                               status_path = '/{0}/jobs'.format(config),
                               test_sets= valid_test_sets)

            #Test Set is Valid, now set up job and run it
            start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            tests = test_set['tests'].split(';')
            job.job_start(test_set['test_set_name'], start_time, len(tests), test_set['poc'], test_set['poc_email'])

            job_id = job.get_job_id(start_time)

            run_multi.run_dq_multi(config_path, job_id, testSet)
            #run_multi.run_test_set(config_path, job_id, testSet)

        else:
            #Config file does not exist
            err_msg = err_config.format(config_path)
            return err_msg

    except:
        print "Unexpected error:", sys.exc_info()[0]
        return "Unexpected error:", sys.exc_info()[0]
        raise

    return render_template("execute_job.html",
                           title=config,
                           logo = logo_url,
                           status_path = '/{0}/jobs'.format(config),
                           test_set= test_set['test_set_name'],  #use value from dict, in case user entered number
                           job_id = job_id,
                           job_path = '/{0}/{1}/results'.format(config,job_id))


#get status of jobs -- http://localhost:5000/Theorem/jobs
@app.route('/<string:config>/jobs', endpoint='jobs')
def get_jobs(config):
    try:
        config_path = "{0}/cfg/{1}.cfg".format(sys.path[0], config)
        #First verify config file exists
        if os.path.isfile(config_path):
            job = test_Module.DqTesting(config_path)
            logo_url = job.logo_url
            jobs = job.retrieve_jobs()
            #Populate URL key with address to connect to specific job results
            for my_job in jobs:
                my_job['url'] = '/{0}/{1}/results'.format(config, my_job['job_id'])
        else:
            err_msg = err_config.format(config_path)
            return err_msg

    except Exception, err:
        err_msg = traceback.format_exc().splitlines()
        return render_template("general_error.html",
               title=config,
               logo = logo_url,
               status_path = '/{0}/jobs'.format(config),
               err_msg = err_msg)

    return render_template("jobs.html",
                           title=config,
                           logo = logo_url,
                           status_path = '/{0}/jobs'.format(config),
                           jobs=jobs)

#get job results -- http://localhost:5000/TheoremDQ/31/results
@app.route('/<string:config>/<string:job_id>/results', endpoint='results')
def job_results(config, job_id):
    try:
        config_path = "{0}/cfg/{1}.cfg".format(sys.path[0], config)
        #First verify config file exists
        if os.path.isfile(config_path):
            job = test_Module.DqTesting(config_path)
            logo_url = job.logo_url
            job_info = job.get_job_info(job_id)
            # Verify job exists
            if job_info is None:
                err_msg = []
                err_msg.append("Job ID: {0} is not a valid Job ID".format(job_id))
                return render_template("general_error.html",
                       title=config,
                       logo = logo_url,
                       status_path = '/{0}/jobs'.format(config),
                       err_msg = err_msg)

            # Calculate % complete
            job_info['percent_comp'] = (float(job_info['tests_complete'])/float(job_info['num_tests'])) * 100

            #if error occurred break it up into line array so it can be viewed in html
            if job_info['error'] is not None and job_info['error'] != '':
                job_info['error'] = job_info['error'].splitlines()
                # Using flag to work better with HTML
                job_info['error_occurred'] = 'True'
            else:
                job_info['error_occurred'] = 'False'

            my_results = job.get_job_results(job_id)
            # Create url for log file if it exists
            for result in my_results:
                if result['log_exists'] == 'True':
                    result['log_path'] = '/{0}/{1}/log'.format(config, result['id'])
        else:
            err_msg = err_config.format(config_path)
            return err_msg
    except Exception, err:
        err_msg = traceback.format_exc().splitlines()
        return render_template("general_error.html",
               title=config,
               logo = logo_url,
               status_path = '/{0}/jobs'.format(config),
               err_msg = err_msg)

    return render_template("results.html",
                           title=config,
                           logo = logo_url,
                           status_path = '/{0}/jobs'.format(config),
                           job=job_info,
                           tests=my_results)


#get notes from specific test -- http://localhost:5000/TheoremDQ/121/log
@app.route('/<string:config>/<string:test_id>/log', endpoint='log')
def test_log(config, test_id):
    try:
        config_path = "{0}/cfg/{1}.cfg".format(sys.path[0], config)
        #First verify config file exists
        if os.path.isfile(config_path):
            job = test_Module.DqTesting(config_path)
            logo_url = job.logo_url
            my_log = job.get_test_log(test_id)
            if my_log is None:
                log_array = ['There is no log for this Test ID']
            else:
                log_array = my_log['log'].splitlines()
        else:
            err_msg = err_config.format(config_path)
            return err_msg

    except Exception, err:
        err_msg = traceback.format_exc().splitlines()
        return render_template("general_error.html",
               title=config,
               logo = logo_url,
               status_path = '/{0}/jobs'.format(config),
               err_msg = err_msg)

    return render_template("test_log.html",
                           title=config,
                           logo = logo_url,
                           status_path = '/{0}/jobs'.format(config),
                           result_path = '/{0}/{1}/results'.format(config, my_log['job_id']),
                           test_id=test_id,
                           log= log_array)

# ------------------------------------------------------------------------
#logging wrapper:
#log_name = datetime.datetime.now().strftime("hambot-%Y%m%d-%H%M%S.log")
#logging.basicConfig(filename=log_name,level=logging.DEBUG, format='%(asctime)s %(message)s',filmode='w')

def log(message):
    logging.info(message)
    print(message)
#------------------------------------------------------------------------
if __name__ == '__main__':
    app.run()
#Valid Arg1 value will call DW job, optional Arg2 boolean will set debug mode
#need to add command line at some point
    # if len(sys.argv)==1:
    #print "HAMbot.py requires arguments for the testModule and testSet. No Action Taken."
    # sys.exit
    # if len(sys.argv)==4:
    # debug=sys.argv[3]
    # else:
    # debug=False

#get_jobs('stuff')
#job_results('Theorem', 84)
#get_jobs('Theorem')
#run('Theorem', 'Simul_Data_Test')
#run('TheoremDQ', 'Timed Tests1'
#test_notes('TheoremDQ', 125)
#http://localhost:5000/Theorem/126/log
#test_log('Theorem', 126)