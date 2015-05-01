__author__ = 'kevin'

from datetime import datetime
import os
import ConfigParser
import sys
import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor
import traceback
from sqlalchemy import create_engine

#exec_path = "%s/%s" % (sys.path[0], 'f_fourthwall_ndh_load.sql')
#mydir = os.path.dirname(os.path.abspath(__file__))
#new_path = os.path.join(mydir, '..', rest_of_the_path)


#exec_path = "%s/%s" % (os.path.dirname(os.path.abspath(__file__)), 'dq.cfg')
#conf = ConfigParser.ConfigParser()
#conf.read('/home/kevin/GitHub/Theorem/ooluroo-data-warehouse/data-quality/dq.cfg')


class DqTesting(object):

    __job_id = None

    def __init__(self, config_file):
        self.conf = ConfigParser.ConfigParser()
        #config_file = "{0}/cfg/{1}.cfg".format(sys.path[0], config_file)
        #if os.path.isfile(config_file):
        self.conf.read(config_file)
        self.logo_url = self.conf.get('general','logo')
        self.scripts_path = self.conf.get('general','scripts_path')
        self.set_db_connection()

    def __del__(self):
        self.close_db_connections()

    # ----------------------------------------------------------------------------------
    # Make database connections, this is called on class instantiation
    def set_db_connection(self):
        try:
            status = 0
            dq_cred = {
                        'dbname': self.conf.get('repository','dbname'),
                        'host': self.conf.get('repository','host'),
                        'user': self.conf.get('repository','user'),
                        'password': self.conf.get('repository','password')
                    }
            self.TestDBConn = psycopg2.connect(**dq_cred)
        except Exception, err:
            print err
            raise

        return status

    # ----------------------------------------------------------------------------------
    # Close database connections, this is called on class deletion
    def close_db_connections(self):
        try:
            status = 0
            self.TestDBConn.close()
        except:
            status = 1

        return status

    # ----------------------------------------------------------------------------------
    # Retrieve results of a specific job, used in tandem with get_job_info
    # to populate results page in HAMbot.
    def get_job_results(self, job_ID):

        strSQL =' select r.id, r.test_id, r.test_name, r.result, r.status, r.upperlim, r.upperwarn, r.lowerwarn, r.lowerlim, r.comp, r.error, t.debug_msg, '\
                ' (CASE WHEN tl.log is null THEN null ELSE \'True\' END) as log_exists, null as log_path'\
                ' from results r join tests t on r.test_id = t.test_id ' \
                ' left outer join test_logs tl on r.id = tl.id '\
                ' where r.job_id = {0} order by r.id desc'.format(job_ID)

        TestDBCur = self.TestDBConn.cursor(cursor_factory=DictCursor)
        TestDBCur.execute(strSQL)
        data = TestDBCur.fetchall()

        return data

    # ----------------------------------------------------------------------------------
    # Retrieve current status of a specific job, used in tandem with get_test_results
    # to populate results page in HAMbot.
    def get_job_info(self, job_ID):

        strSQL='select * from jobs where job_id = \'{0}\''.format(job_ID)

        TestDBCur = self.TestDBConn.cursor(cursor_factory=RealDictCursor)
        TestDBCur.execute(strSQL)
        data = TestDBCur.fetchone()

        return data

    # ----------------------------------------------------------------------------------
    # Retrieve Test Set from DQ database
    def get_test_set(self, test_set):
        try:
            #Determine whether user entered Test_Set_ID or Test_Set_Name
            if is_number(test_set):
                strSQL='select * from test_set where test_set_id = \'{0}\''.format(test_set)
            else:
                strSQL='select * from test_set where test_set_name = \'{0}\''.format(test_set)

            TestDBCur = self.TestDBConn.cursor(cursor_factory=RealDictCursor)
            TestDBCur.execute(strSQL)
            data = TestDBCur.fetchone()

        except Exception, err:
            print err
            raise

        return data

    # ----------------------------------------------------------------------------------
    # Make the necessary DB connections and import modules - performs this for each test set
    def make_connections(self, tests):
        try:
            tests = tests.replace(';',',')

            strSQL="""SELECT type1 FROM tests
                    WHERE test_id IN (5003,5001,5002)
                    AND type1 is not null
                    UNION SELECT type1 FROM tests
                    WHERE test_id IN (select action_on_pass from tests where test_id in (5003,5001,5002) and action_on_pass is not null)
                    AND type1 is not null
                    UNION SELECT type1 FROM tests
                    WHERE test_id IN (select action_on_fail from tests where test_id in (5003,5001,5002) and action_on_fail is not null)
                    AND type1 is not null
                    UNION SELECT type2 FROM tests
                    WHERE test_id IN (5003,5001,5002)
                    AND type2 is not null
                    UNION SELECT type2 FROM tests
                    WHERE test_id IN (select action_on_pass from tests where test_id in (5003,5001,5002) and action_on_pass is not null)
                    AND type2 is not null
                    UNION SELECT type2 FROM tests
                    WHERE test_id IN (select action_on_fail from tests where test_id in (5003,5001,5002) and action_on_fail is not null)
                    AND type2 is not null;""".format(tests)

            TestDBCur = self.TestDBConn.cursor()
            TestDBCur.execute(strSQL)
            conn_array = TestDBCur.fetchall()

            for conn in conn_array:
                name = conn[0]
                my_dict = {}
                if name is not None and name != '':
                    my_type= self.conf.get(name,'type')

                    if my_type.upper() == 'SQL':
                        my_dict = {'sqlalchemy_url': self.conf.get(name,'sqlalchemy_url'),
                                'type': self.conf.get(name,'type')}
                        eng = create_engine(my_dict['sqlalchemy_url'])
                        my_dict['connection'] = eng.connect()

                    elif my_type.upper() == 'PYTHON':
                        my_dict = {'module_name': self.conf.get(name,'mod_name'),
                                'type': self.conf.get(name,'type')}
                        my_dict['pyModule'] = __import__(my_dict['module_name'])
                    else:
                        raise NameError('Type: {0} cannot found in the config')

                    my_dict['active'] = True
                    setattr(self, name, my_dict)

        except Exception, err:
            print err
            raise

    # ----------------------------------------------------------------------------------
    # Retrieve All Test Sets from DQ database, used to populate page if invalid test_set entered
    def get_all_test_sets(self):
        try:
            #Determine whether user entered Test_Set_ID or Test_Set_Name
            strSQL='select test_set_name, null as url from test_set'
            TestDBCur = self.TestDBConn.cursor(cursor_factory=DictCursor)
            TestDBCur.execute(strSQL)
            data = TestDBCur.fetchall()
        except Exception, err:
            print err
            raise

        return data

    # ----------------------------------------------------------------------------------
    # Retrieve a specific Test from DQ database
    def get_test(self, test):
        try:
            strSQL='select * from tests where test_id = {0}'.format(test)

            TestDBCur = self.TestDBConn.cursor(cursor_factory=RealDictCursor)
            TestDBCur.execute(strSQL)
            data = TestDBCur.fetchone()

        except Exception, err:
            print err
            raise

        return data

    # ----------------------------------------------------------------------------------
    # Retrieve the log associated with a specific test
    def get_test_log(self, test_id):
        try:
            strSQL='select tl.log, r.job_id '\
                    ' from results r inner join test_logs tl on r.id = tl.id '\
                    ' where r.id = {0}'.format(test_id)

            TestDBCur = self.TestDBConn.cursor(cursor_factory=RealDictCursor)
            TestDBCur.execute(strSQL)
            data = TestDBCur.fetchone()

        except Exception, err:
            print err
            raise

        return data

    # ----------------------------------------------------------------------------------
    # Retrieve all tests that failed from a job
    def get_failed_tests(self, job):
        try:
            strSQL='select test_id, test_name, result, upperlim, lowerlim, comp, status, error '\
                    ' from results where job_id = {0} and status != \'PASSED\''.format(job)

            TestDBCur = self.TestDBConn.cursor(cursor_factory=DictCursor)
            TestDBCur.execute(strSQL)
            data = TestDBCur.fetchall()

        except Exception, err:
            print err
            raise

        return data

    # ----------------------------------------------------------------------------------
    # Runs the test
    def run_test(self, test):
        try:
            #Initialize Test Dictionary
            test_dict = {}
            self.test_log = []

            # Retrieve test for db as real dictionary
            test_dict = self.get_test(test)

            # Add error, result, and status key to test_dict
            test_dict['error'] = ''
            test_dict['result'] = ''
            test_dict['status'] = ''

            # Update job
            self.job_update(test_dict['test_id'], test_dict['test_name'])

            # Log notes
            self.test_log.append('Running Test: ' + test_dict['test_name'])
            self.test_log.append('Test Start Time: ' + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            if test_dict is not None:
                # Log Script 1 info
                self.test_log.append('Script 1-> Name: {0}    Type: {1}    Args: {2}'.format(test_dict['script1'],test_dict['type1'],test_dict['args1']))
                # Create args array
                if test_dict['args1'] is None or test_dict['args1'] == '':
                    args = None
                else:
                    args = test_dict['args1'].split(';')

                #Execute script
                test_dict['result_1'] = self.execute_script(test_dict['type1'], test_dict['script1'], args)
                # Run second test if defined
                if test_dict['script2'] == '' or test_dict['script2'] is None:
                    test_dict['result_2'] = None

                else:
                    # Log Script 2 info
                    self.test_log.append('Script 2-> Name: {0}    Type: {1}    Args: {2}'.format(test_dict['script2'],test_dict['type2'],test_dict['args2']))

                    if test_dict['args2'] is None or test_dict['args2'] == '':
                        args = None
                    else:
                        args = test_dict['args2'].split(';')

                    test_dict['result_2'] = self.execute_script(test_dict['type2'], test_dict['script2'], args)

                # Create sub dictionary to pass into get_test_status
                sub_list = ['result_1', 'result_2', 'upperlim', 'upperwarn', 'lowerwarn', 'lowerlim', 'comp','result_type', 'error']
                temp_dict =  {k:v for k,v in test_dict.items() if k in sub_list}
                test_dict['status'], test_dict['result'] = self.set_test_status(**temp_dict)

                self.test_log.append('Test End Time: ' + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

                #now write to database
                sub_list = ['test_id','test_name','result', 'status', 'upperlim', 'upperwarn', 'lowerwarn', 'lowerlim', 'comp', 'error', 'log_test']
                temp_dict =  {k:v for k,v in test_dict.items() if k in sub_list}
                self.write_results(**temp_dict)
            else:
                pass
                #test not found, throw some error of fill test_dict with info stating test doesn't exist

        except Exception, err:
            # update test_dict with short error
            test_dict['error']= 'ERROR: %s\n' % str(err)
            test_dict['status'] = 'Error'
            test_dict['result'] = -999
            test_dict['log_test'] = 'Verbose'
            # Log traceback stack
            self.test_log.append("TEST ERROR:")
            self.test_log.append(traceback.format_exc())
            # Write to database
            sub_list = ['test_id','test_name','result', 'status', 'upperlim', 'upperwarn', 'lowerwarn', 'lowerlim', 'comp', 'error', 'log_test']
            temp_dict =  {k:v for k,v in test_dict.items() if k in sub_list}
            self.write_results(**temp_dict)
            return test_dict

        #Return test_dict back to Hambot, will be used to monitor Job ID pass/fail, and generate email text if necessary
        return test_dict

    # ----------------------------------------------------------------------------------
    # Determines what type of script is being called based on type stored in tests table
    def execute_script(self, type, name, args):
        try:
            # Get the dictionary created in make_connections based off of config file
            type_dict = getattr(self, type)
            return_result = []

            if type_dict['type'].upper() == "SQL":
                result = self.sql_wrap(type_dict['connection'], name, args)

            elif type_dict['type'].upper() == "PYTHON":
                result = self.py_wrap(type_dict['pyModule'], name, args)

            else:
                 raise Exception("Invalid script type: %s " % type_dict['type'].upper())

            #Hambot expects all results as a list
            if not isinstance(result, list):
                return_result.append(result)
            else:
                return_result = result

            #Log raw results
            self.test_log.append('Raw Returned Results: ')
            for i in return_result:
                self.test_log.append(str(i))

        except Exception, err:
            print err
            raise

        return return_result

    # ----------------------------------------------------------------------------------
    # Retrieves method from a python module and runs it with parameters (optional)
    def py_wrap(self, module, name, args):
        try:
            self.test_log.append('Python Module: {0}    Running method: {1}    Parameters: {2}'.format(module, name, args))

            method = getattr(module, name)
            if not method:
                raise Exception("Method %s not implemented" % name)
            if args is not None:
                result = method(*args)
            else:
                result = method()

        except Exception, err:
            print err
            raise

        return result

    # ----------------------------------------------------------------------------------
    # Retrieves sqlscript from a python module and runs it with parameters (optional)
    def sql_wrap(self, dbConnection, name, args):
        try:
            #Remove white space in sql script
            name = name.strip()
            exec_path = "%s/%s" % (self.scripts_path, name)
            self.test_log.append('SQL Execution Path: {0}'.format(exec_path))
            exec_str = open(exec_path, 'r').read()
            if args is not None:
                exec_str = exec_str.format(*args)
            strSQL = exec_str

            #Log SQL Query
            self.test_log.append('SQL Query:')
            self.test_log.append(strSQL)

            result = dbConnection.execute(strSQL)
            new_data = result.fetchall()
            result.close
            #SQLALchemy returns RowProxy object, which is supposed to be a tuple,
            # but doesn't qualify as one in isinstance()
            # need to cast as tuple in order to work with clean_result function
            data=[]
            for row in new_data:
                data = tuple(row)

        except Exception, err:
            print err
            raise

        return data

    # ----------------------------------------------------------------------------------
    # retrieves job id based on start time, used in tandem with job_start
    def get_job_id(self, start_time):
        try:
            TestDBCur = self.TestDBConn.cursor()
            strSQL = 'select job_id from jobs where start_time = \'{0}\''.format(start_time)
            TestDBCur.execute(strSQL)
            job_id = TestDBCur.fetchone()

        except Exception, err:
            print err
            raise

        self.job_id = job_id[0]
        return job_id[0]

    # ----------------------------------------------------------------------------------
    # retrieves job id based on start time, used in tandem with job_start
    def set_job_id(self, job_id):
        try:
            status = 0
            self.job_id = job_id

        except Exception, err:
            print err
            raise

        return status

    # ----------------------------------------------------------------------------------
    # writes job start data into jobs table.  After this call a new job_id will be produced
    # follow this with calling get job
    def job_start(self, job_name, start_time, num_tests, poc, poc_email):
        status = 0
        try:
            TestDBCur = self.TestDBConn.cursor()

            strSQL = 'insert into jobs (job_name, start_time, num_tests, poc, poc_email) ' \
                     'select \'{0}\', to_timestamp(\'{1}\', \'YYYY-MM-DD HH24:MI:SS\'), {2}, \'{3}\', \'{4}\'; commit;'\
                     .format(job_name, start_time, num_tests, poc, poc_email)

            TestDBCur.execute(strSQL)

        except Exception, err:
            print err
            raise

        return status

    # ----------------------------------------------------------------------------------
    # update job - increment test complete, define current test
    def job_update(self, current_test, current_test_name):
        try:
            status = 0
            TestDBCur = self.TestDBConn.cursor()

            strSQL = 'update jobs set status = \'Running...\', current_test = {0}, current_test_name = \'{1}\' where job_id = {2};commit;'\
                .format(current_test, current_test_name, self.job_id)

            TestDBCur.execute(strSQL)

        except Exception, err:
            print err
            raise

        return status

    # ----------------------------------------------------------------------------------
    # job is complete, update status and end time
    def job_complete(self, status, end_time, error = ''):
        try:
            my_status = 0
            TestDBCur = self.TestDBConn.cursor()

            strSQL = 'update jobs set status = \'{0}\', end_time = to_timestamp(\'{1}\', \'YYYY-MM-DD HH24:MI:SS\'), error = \'{2}\' where job_id = {3};commit;'\
                .format(status, end_time, error, self.job_id)

            TestDBCur.execute(strSQL)

        except Exception, err:
            print err
            raise

        return my_status

    # ----------------------------------------------------------------------------------
    # write results to results table
    def write_results(self, test_id, test_name, result, status, upperlim, upperwarn, lowerwarn, lowerlim, comp, error, log_test):
        try:
            # Make sure none of the parameters being written are 'None'
            if upperlim is None: upperlim =''
            if upperwarn is None: upperwarn =''
            if lowerwarn is None: lowerwarn =''
            if lowerlim is None: lowerlim =''

            my_status = 0
            TestDBCur = self.TestDBConn.cursor()

            strSQL = 'insert into results (job_id, test_id, test_name, result, status, upperlim, upperwarn, lowerwarn, lowerlim, comp, error) '\
                     'select {0}, {1}, \'{2}\', {3}, \'{4}\', \'{5}\', \'{6}\', \'{7}\', \'{8}\',\'{9}\',\'{10}\' RETURNING id;'\
                     .format(self.job_id, test_id, test_name, result, status, upperlim, upperwarn, lowerwarn, lowerlim, comp, error)

            TestDBCur.execute(strSQL)
            log_id = TestDBCur.fetchone()[0]

            #Log test notes if necessary
            if log_test.upper() != "NO" or status == "FAILED":
                log = '\n'.join(self.test_log)
                new_log = log.replace('\'', '')
                strSQL2 = 'insert into test_logs (id, log) select {0}, \'{1}\'; commit;'.format(log_id, new_log)
                TestDBCur.execute(strSQL2)
            else:
                #if not logging, need to commit previous insert
                TestDBCur.execute('commit;')

            # increment tests complete once results are written
            strSQL3= 'update jobs set tests_complete = tests_complete + 1 where job_id = {0};commit;'\
                .format(self.job_id)

            TestDBCur.execute(strSQL3)

        except Exception, err:
            print err
            raise

        return my_status

    # ----------------------------------------------------------------------------------
    # determine whether test passed/failed based on its parameters
    def set_test_status(self, result_1, result_2, upperlim, upperwarn, lowerwarn, lowerlim, comp, result_type, error):

        try:
            # Cast Limits - if limit is empty string '' will return as none
            upperlim = cast_limit(result_type, upperlim)
            upperwarn = cast_limit(result_type, upperwarn)
            lowerwarn= cast_limit(result_type, lowerwarn)
            lowerlim= cast_limit(result_type, lowerlim)

            if result_2 is None:
                result = clean_result(result_1)
            else:
                new_result_1 = clean_result(result_1)
                new_result_2 = clean_result(result_2)
                # use length of smallest array to not throw an indexing error
                my_range = min(len(new_result_1),len(new_result_2))
                result = []
                # get results array
                for i in range(0,my_range):
                    if comp.upper() == "VALUE":
                        result.append(new_result_1[i] - new_result_2[i])
                    elif comp.upper() == "PERCENT":
                        result.append((1-(float(new_result_1[i])/float(new_result_2[i]))) * 100)
                    elif comp.upper() == "STRING":
                        # do something for string compare
                        result = 'String'
                    else:
                        status = "ERROR"
                        print "Invalid Compare Type {0}, in get_test_status".format(comp)
                        raise

            my_status=[]
            overall_status = "PASSED"
            self.test_log.append('Test Results:')
            self.test_log.append('UpperLimit = {0}   UpperWarn = {1}   LowerWarn = {2}   LowerLimit = {3}   Comparison = {4}'.format(upperlim, upperwarn, lowerwarn, lowerlim, comp))

            #populate status array
            for index, value in enumerate(result):
                temp_status = 'PASSED'
                # Re-written for simplicity and flexibility - now allows for 0-4 limits to be defined
                if upperlim is not None:
                    if value > upperlim: temp_status = "FAILED"

                if temp_status != "FAILED" and lowerlim is not None:
                    if value < lowerlim: temp_status = "FAILED"

                if temp_status != "FAILED" and upperwarn is not None:
                    if value > upperwarn: temp_status = "WARNING"

                if temp_status != "FAILED" and temp_status != "WARNING" and lowerwarn is not None:
                    if value < lowerwarn: temp_status = "WARNING"

                my_status.append(temp_status)
                if temp_status == "FAILED":
                    overall_status = "FAILED"
                elif temp_status == "WARNING" and overall_status != "FAILED":
                    overall_status = "WARNING"

                #Log results and status.  If there are two scripts log all results
                if result_2 is None:
                    self.test_log.append('Result: {0}   Status:  {1}'.format(value, my_status[-1]))
                else:
                    self.test_log.append('Return 1: {0}    Return 2: {1}    Result: {2}    Status: {3}'.format(new_result_1[index], new_result_2[index], value, my_status[-1]))

            #Report first error, warning, or pass
            index = my_status.index(overall_status)
            return_result = result[index]

        except Exception, err:
            print err
            raise

        return overall_status, return_result

    # ----------------------------------------------------------------------------------
    # retrieves all job in the last 14 days. Used to populate status page in HAMbot
    def retrieve_jobs(self):
         try:
            my_status = 0

            TestDBCur = self.TestDBConn.cursor(cursor_factory=DictCursor)

            strSQL = 'select job_id, job_name, start_time, (tests_complete/num_tests) * 100 as percent_complete,'\
                     ' to_char(coalesce(end_time, CURRENT_TIMESTAMP) -start_time, \'MI:SS\') as duration, current_test_name, status, null as url '\
                     ' from jobs where start_time > CURRENT_TIMESTAMP - INTERVAL \'50 days\' order by start_time desc'

            TestDBCur.execute(strSQL)
            data = TestDBCur.fetchall()

         except Exception, err:
            print err
            raise

         return data

# ----------------------------------------------------------------------------------
# determine if value is a number
def is_number(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

# ----------------------------------------------------------------------------------
# this function goes through the a list and converts any tuple in the list
# to a list and returns the first value.  This is used for comparing results of SQL queries
def clean_result(result_list):
    my_result = []
    for i in result_list:
        #convert tuple to list and return first value
        if isinstance(i, tuple):
            temp = list(i)
            val = temp[0]
        else:
            val = i
        my_result.append(val)
    return my_result

# ----------------------------------------------------------------------------------
# this function will cast the limits to the type specified in result type
def cast_limit(type, value):
    try:
        if value is None or value == '':
            my_value = None
        else:
            if type == 'int':
                my_value = int(value)
            elif type == 'str':
                my_value = str(value)
            elif type == 'float':
                my_value = float(value)
            else:
                raise Exception("Invalid cast type: {0} in function cast_limit".format(type))

    except Exception, err:
            print err
            raise

    return my_value

#------------------------------------------------------------------------
#if __name__ == '__main__':
#    app.run()


#job = DqTesting('Theorem')
#job.set_job_id(37)
#test_set = job.get_test_set(3)
#job.make_connections(test_set['tests'])
#job.run_test(1001)
#print job.mysql

#log =job.get_test_log(121)
#log_array = log['log'].splitlines()
#log_array=log_str.splitlines()
#print log_array
#job.get_test_results(48)
#job.set_job_id(37)

#job.run_test(3002)
#job.execute_script(2, 'DQ_Campaign_Duplicates1.sql', [])

#cast_limit(int, '10')
#overall_status, return_result = job.set_test_status([30], None, None, 29, None, None, 'Value', int, '')
#job.execute_script('stuff', 'stuff', 'stuff')