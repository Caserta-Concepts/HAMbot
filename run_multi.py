__author__ = 'kevin'

import os
import sys
import multiprocessing
from email.mime.text import MIMEText
from datetime import datetime
import smtplib
import traceback
import hambot_test_class as test_Module
import requests
import ConfigParser

def run_dq_multi(config_path, job_id, test_set_name):
    p = multiprocessing.Process(target=run_test_set, args=(config_path, job_id, test_set_name))
    p.start()


def run_test_set(config_path, job_id, test_set_name):
    try:
        #Get name of config to be used for labeling further on
        config = os.path.basename(config_path)
        config = config.split('.')[0]
        #Instantiate DqTesting class and set the job_ID
        job = test_Module.DqTesting(config_path)
        job.set_job_id(job_id)

        test_set = job.get_test_set(test_set_name)

        # Read the types used in the tests to make any db connections or import modules
        job.make_connections(test_set['tests'])

        tests = test_set['tests'].split(';')

        job_status = 'PASSED'

        for test in tests:
            result_dict = {}
            result_dict = job.run_test(test)

            # update job pass/fail status and run action if it exists
            if result_dict['status'] == 'PASSED':
                if result_dict['action_on_pass'] is not None:
                        job.run_test(result_dict['action_on_pass'])

            elif result_dict['status'] == 'FAILED':
                job_status = 'FAILED'

                # Run action if defined
                if result_dict['action_on_fail'] is not None:
                    job.run_test(result_dict['action_on_fail'])

                # Stop of failure if flag is set
                if test_set['stop_on_fail'] is True:
                    break

            elif result_dict['status'] == 'WARNING' and job_status != 'FAILED':
                job_status = 'WARNING'

        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        job.job_complete(job_status, end_time)

        if job_status != 'PASSED':
            failed_tests = job.get_failed_tests(job_id)
            email_body = []
            # Make header first
            email_body.append(get_report_record('Test ID', 'Test Name', 'Result', 'Upper', 'Lower', 'Comp', 'Status', ''))
            # Email body
            for test in failed_tests:
               email_body.append(get_report_record(**test))

            email_body = ''.join(email_body)
            send_email(config, job_id, test_set['test_set_name'], test_set['poc'], test_set['poc_email'], email_body)

    except Exception, err:
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        temp_err = traceback.format_exc()
        err_str = temp_err.replace('\'', '')
        job.job_complete("ERROR", end_time, err_str)
        job.close_db_connections()
        print err

    job.close_db_connections()

#------------------------------------------------------------------------
def get_report_record(test_id, test_name, result, upperlim, lowerlim, comp, status, error):

    test_id = add_padding(str(test_id), 10)
    test_name = add_padding(test_name, 40)
    result = add_padding(str(result), 10)
    upperlim = add_padding(str(upperlim), 10)
    lowerlim = add_padding(str(lowerlim), 10)
    comp = add_padding(comp, 10)
    status = add_padding(status, 10)
    if error != '' and error is not None:
        record = test_id + '  ' + test_name + '  ' + 'Error: ' + error
    else:
        record = test_id + '  ' + test_name + '  ' + result + '  ' + comp + '  ' + lowerlim + '  ' + upperlim + '  ' + status + '\n'

    return record

#------------------------------------------------------------------------
def add_padding(str, length):

    if str != "" and length > 0:
        if len(str) > length:
            str = str[:length -1]
        else:
            str = "{0:{1}}".format(str, length)
    return str

# ------------------------------------------------------------------------
def send_email(testModule, job, test_set, send_to_name, send_to_email, body):
     SMTP_SERVER = "smtp.gmail.com"
     SMTP_PORT = 587
     SMTP_USERNAME = "some email"
     SMTP_PASSWORD = "your password"

     EMAIL_TO = [send_to_email]
     EMAIL_FROM = "your email"
     EMAIL_SUBJECT = 'Alert Notification: {0} - Job {1} Failed'.format(testModule, job)

     EMAIL_SPACE = ", "
     intro = '{0},\n\nThis email has been sent to notify you that Job: {1} failed while running Test Set: {2}.'\
             '\n\nThe failing tests are listed below:\n\n\n'.format(send_to_name, job, test_set)
     DATA=intro + body


     msg = MIMEText(DATA)
     msg['Subject'] = EMAIL_SUBJECT
     msg['To'] = EMAIL_SPACE.join(EMAIL_TO)
     msg['From'] = EMAIL_FROM
     mail = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
     mail.ehlo()
     mail.starttls()
     mail.login(SMTP_USERNAME, SMTP_PASSWORD)
     mail.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
     mail.quit()



