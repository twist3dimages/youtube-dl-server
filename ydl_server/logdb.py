# import mysql.connector
# import re
# import datetime
# import os
# from ydl_server.config import app_config
# ###############################################
# # Constants
# STATUS_NAME = ["Running", "Completed", "Failed", "Pending", "Aborted"]

# # Database connection parameters from Docker environment variables
# DB_HOST = os.environ.get('DB_HOST', 'localhost')
# DB_PORT = os.environ.get('DB_PORT', 3306)
# DB_NAME = os.environ.get('DB_NAME', 'your_database_name')
# DB_USER = os.environ.get('DB_USER', 'your_database_user')
# DB_PASSWORD = os.environ.get('DB_PASSWORD', 'your_database_password')

# # MariaDB connection
# def get_db_connection():
#     return mysql.connector.connect(
#         host=DB_HOST,
#         port=DB_PORT,
#         database=DB_NAME,
#         user=DB_USER,
#         password=DB_PASSWORD
#     )
# class Actions:
#     DOWNLOAD = 1
#     PURGE_LOGS = 2
#     INSERT = 3
#     UPDATE = 4
#     RESUME = 5
#     SET_NAME = 6
#     SET_STATUS = 7
#     SET_LOG = 8
#     CLEAN_LOGS = 9
#     SET_PID = 10
#     DELETE_LOG = 11


# class JobType:
#     YDL_DOWNLOAD = 0
#     YDL_UPDATE = 1


# class Job:
#     RUNNING = 0
#     COMPLETED = 1
#     FAILED = 2
#     PENDING = 3
#     ABORTED = 4

#     def __init__(self, name, status, log, jobtype, format=None, url=None, id=-1, pid=0):
#         self.id = id
#         self.name = name
#         self.status = status
#         self.log = log
#         self.last_update = ""
#         self.format = format
#         self.type = jobtype
#         self.url = url
#         self.pid = pid

#     @staticmethod
#     def clean_logs(logs):
#         if not logs:
#             return logs
#         clean = ""
#         for line in logs.split("\n"):
#             line = re.sub(".*\r", "", line)
#             if len(line) > 0:
#                 clean = "%s%s\n" % (clean, line)
#         return clean



# # Updated JobsDB class for MariaDB
# class JobsDB:
#     @staticmethod
#     def check_db_latest():
#         conn = get_db_connection()
#         cursor = conn.cursor()
#         cursor.execute("SHOW TABLES LIKE 'jobs'")
#         if cursor.fetchone() is None:
#             print("Outdated jobs table, cleaning up and recreating")
#             cursor.execute("DROP TABLE IF EXISTS jobs;")
#             JobsDB.init_db()
#         conn.close()

#     @staticmethod
#     def init_db():
#         conn = get_db_connection()
#         cursor = conn.cursor()
#         cursor.execute(
#             """
#             CREATE TABLE IF NOT EXISTS jobs
#                 (
#                     id INT AUTO_INCREMENT PRIMARY KEY,
#                     name TEXT NOT NULL,
#                     status INT NOT NULL,
#                     log TEXT,
#                     format TEXT,
#                     last_update DATETIME DEFAULT CURRENT_TIMESTAMP,
#                     type INT NOT NULL,
#                     url TEXT,
#                     pid INT
#                 );
#             """
#         )
#         conn.commit()
#         conn.close()
#     def convert_datetime_to_tz(dt):
#         dt = datetime.datetime.strptime("{} +0000".format(dt), "%Y-%m-%d %H:%M:%S %z")
#         return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")

#     def __init__(self, readonly=True):
#         self.readonly = readonly
#         self.conn = mysql.connector.connect(
#             host=os.environ.get('DB_HOST', 'localhost'),
#             port=os.environ.get('DB_PORT', 3306),
#             database=os.environ.get('DB_NAME', 'your_database_name'),
#             user=os.environ.get('DB_USER', 'your_database_user'),
#             password=os.environ.get('DB_PASSWORD', 'your_database_password')
#         )
#         if readonly:
#             self.conn.autocommit = False
#         else:
#             self.conn.autocommit = True


#     def close(self):
#         self.conn.close()

#     # def insert_job(self, job):
#     #     cursor = self.conn.cursor()
#     #     cursor.execute(
#     #         """
#     #         INSERT INTO jobs
#     #             (name, status, log, format, type, url, pid)
#     #         VALUES
#     #             (%s, %s, %s, %s, %s, %s, %s);
#     #         """,
#     #         (
#     #             job.name,
#     #             job.status,
#     #             job.log,
#     #             job.format,
#     #             job.type,
#     #             "\n".join(job.url),
#     #             job.pid,
#     #         ),
#     #     )
#     #     job.id = cursor.lastrowid
#     #     self.conn.commit()
#     def insert_job(self, job):
#         # Filter out log lines containing "0x154f6a7ff680"
#         job.log = '\n'.join(line for line in job.log.split('\n') if "0x154f6a7ff680" not in line)

#         cursor = self.conn.cursor()
#         cursor.execute(
#             """
#             INSERT INTO jobs
#                 (name, status, log, format, type, url, pid)
#             VALUES
#                 (%s, %s, %s, %s, %s, %s, %s);
#             """,
#             (
#                 job.name,
#                 job.status,
#                 job.log,
#                 job.format,
#                 job.type,
#                 "\n".join(job.url),
#                 job.pid,
#             ),
#         )
#         job.id = cursor.lastrowid
#         self.conn.commit()

#     # def update_job(self, job):
#     #     cursor = self.conn.cursor()
#     #     cursor.execute(
#     #         """
#     #         UPDATE jobs
#     #         SET status = %s, log = %s, last_update = NOW()
#     #         WHERE id = %s;
#     #         """,
#     #         (job.status, job.log, job.id),
#     #     )
#     #     self.conn.commit()
#     def update_job(self, job):
#         # Filter out log lines containing "0x154f6a7ff680"
#         job.log = '\n'.join(line for line in job.log.split('\n') if "0x154f6a7ff680" not in line)

#         cursor = self.conn.cursor()
#         cursor.execute(
#             """
#             UPDATE jobs
#             SET status = %s, log = %s, last_update = NOW()
#             WHERE id = %s;
#             """,
#             (job.status, job.log, job.id),
#         )
#         self.conn.commit()
#     def set_job_status(self, job_id, status):
#         cursor = self.conn.cursor()
#         cursor.execute(
#             """
#             UPDATE jobs
#             SET status = %s, last_update = NOW()
#             WHERE id = %s;
#             """,
#             (status, job_id),
#         )
#         self.conn.commit()


#     def set_job_pid(self, job_id, pid):
#         cursor = self.conn.cursor()
#         cursor.execute(
#             """
#             UPDATE jobs
#             SET pid = %s, last_update = NOW() \
#             WHERE id = %s;
#             """,
#             (str(pid), str(job_id)),
#         )
#         self.conn.commit()
#     def set_job_log(self, job_id, log):
#         truncated_log = log[-2500:] if len(log) > 2500 else log
#         cursor = self.conn.cursor()
#         cursor.execute(
#             """
#             UPDATE jobs
#             SET log = %s, last_update = NOW()
#             WHERE id = %s;
#             """,
#             (truncated_log, job_id),
#         )
#         self.conn.commit()
# #    def set_job_log(self, job_id, log):
# ##        cursor = self.conn.cursor()
#  #       cursor.execute(
# #            """
# #            UPDATE jobs
# #            SET log = %s, last_update = NOW()
# #            WHERE id = %s;
# #            """,
# #            (log, job_id)  # Use the full log directly
# #        )
# #        self.conn.commit()
    



#     def set_job_name(self, job_id, name):
#         cursor = self.conn.cursor()
#         cursor.execute(
#             """
#             UPDATE jobs
#             SET name = %s, last_update = NOW() \
#             WHERE id = %s;
#             """,
#             (name, str(job_id)),
#         )
#         self.conn.commit()

#     def purge_jobs(self):
#         cursor = self.conn.cursor()
#         cursor.execute("DELETE FROM jobs;")
#         self.conn.commit()
#         self.conn.execute("VACUUM")

#     def delete_job(self, job_id):
#         cursor = self.conn.cursor()
#         cursor.execute(
#             "DELETE FROM jobs WHERE id = %s AND ( status = %s OR status = %s );",
#             (str(job_id), Job.ABORTED, Job.FAILED),
#         )
#         self.conn.commit()
#         self.conn.execute("VACUUM")

#     def clean_old_jobs(self, limit=10):
#         cursor = self.conn.cursor()
#         cursor.execute(
#             """
#             SELECT last_update
#             FROM jobs
#             ORDER BY last_update DESC
#             LIMIT %s;
#             """,
#             (limit,)  # No need to convert limit to string
#         )
#         rows = list(cursor.fetchall())
#         if len(rows) > 0:
#             cursor.execute(
#                 "DELETE FROM jobs WHERE last_update < %s AND status != %s and status != %s;",
#                 (rows[-1][0], Job.PENDING, Job.RUNNING),
#             )
#         self.conn.commit()


#     def get_job_by_id(self, job_id):
#         cursor = self.conn.cursor()
#         cursor.execute(
#             """
#             SELECT
#                 id, name, status, log, last_update, format, type, url, pid
#             FROM
#                 jobs
#             WHERE id = %s;
#             """,
#             (job_id,),
#         )
#         row = cursor.fetchone()
#         if not row:
#             return
#         (
#             job_id,
#             name,
#             status,
#             log,
#             last_update,
#             format,
#             jobtype,
#             url,
#             pid,
#         ) = row
#         return {
#             "id": job_id,
#             "name": name,
#             "status": STATUS_NAME[status],
#             "log": log,
#             "format": format,
#             "last_update": JobsDB.convert_datetime_to_tz(last_update),
#             "type": jobtype,
#             "urls": url.split("\n"),
#             "pid": pid,
#         }

#     def get_all(self, limit=50):
#         cursor = self.conn.cursor()
#         cursor.execute(
#             """
#             SELECT
#                 id, name, status, log, last_update, format, type, url, pid
#             FROM
#                 jobs
#             ORDER BY last_update DESC LIMIT %s;
#             """,
#             (limit,)  # No need to convert limit to string
#         )
#         rows = []
#         for (
#             job_id,
#             name,
#             status,
#             log,
#             last_update,
#             format,
#             jobtype,
#             url,
#             pid,
#         ) in cursor.fetchall():
#             rows.append(
#                 {
#                     "id": job_id,
#                     "name": name,
#                     "status": STATUS_NAME[status],
#                     "log": log,
#                     "format": format,
#                     "last_update": JobsDB.convert_datetime_to_tz(last_update),
#                     "type": jobtype,
#                     "urls": url.split("\n"),
#                     "pid": pid,
#                 }
#             )
#         return rows

#     def get_jobs(self, limit=50):
#         cursor = self.conn.cursor()
#         cursor.execute(
#             """
#             SELECT
#                 id, name, status, last_update, format, type, url, pid
#             FROM
#                 jobs
#             ORDER BY last_update DESC LIMIT %s;
#             """,
#             (limit,)  # No need to convert limit to string
#         )
#         rows = []
#         for (
#             job_id,
#             name,
#             status,
#             last_update,
#             format,
#             jobtype,
#             url,
#             pid,
#         ) in cursor.fetchall():
#             rows.append(
#                 {
#                     "id": job_id,
#                     "name": name,
#                     "status": STATUS_NAME[status],
#                     "format": format,
#                     "last_update": JobsDB.convert_datetime_to_tz(last_update),
#                     "type": jobtype,
#                     "urls": url.split("\n"),
#                     "pid": pid,
#                 }
#             )
#         return rows
import mysql.connector
import re
import datetime
import os
import logging
from ydl_server.config import app_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
STATUS_NAME = ["Running", "Completed", "Failed", "Pending", "Aborted"]

# Database connection parameters from Docker environment variables
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = os.environ.get('DB_PORT', 3306)
DB_NAME = os.environ.get('DB_NAME', 'your_database_name')
DB_USER = os.environ.get('DB_USER', 'your_database_user')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'your_database_password')

# MariaDB connection
def get_db_connection():
    logging.info("Establishing database connection")
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

class Actions:
    DOWNLOAD = 1
    PURGE_LOGS = 2
    INSERT = 3
    UPDATE = 4
    RESUME = 5
    SET_NAME = 6
    SET_STATUS = 7
    SET_LOG = 8
    CLEAN_LOGS = 9
    SET_PID = 10
    DELETE_LOG = 11

class JobType:
    YDL_DOWNLOAD = 0
    YDL_UPDATE = 1

class Job:
    RUNNING = 0
    COMPLETED = 1
    FAILED = 2
    PENDING = 3
    ABORTED = 4

    def __init__(self, name, status, log, jobtype, format=None, url=None, id=-1, pid=0):
        self.id = id
        self.name = name
        self.status = status
        self.log = log
        self.last_update = ""
        self.format = format
        self.type = jobtype
        self.url = url
        self.pid = pid

    @staticmethod
    def clean_logs(logs):
        if not logs:
            return logs
        clean = ""
        for line in logs.split("\n"):
            line = re.sub(".*\r", "", line)
            if len(line) > 0:
                clean = "%s%s\n" % (clean, line)
        return clean

# Updated JobsDB class for MariaDB with logging
class JobsDB:
    @staticmethod
    def check_db_latest():
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES LIKE 'jobs'")
        if cursor.fetchone() is None:
            logging.info("Outdated jobs table, cleaning up and recreating")
            cursor.execute("DROP TABLE IF EXISTS jobs;")
            JobsDB.init_db()
        conn.close()
    def convert_datetime_to_tz(dt):
        try:
            # Validate if the data is in the expected datetime format
            datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            dt = datetime.datetime.strptime("{} +0000".format(dt), "%Y-%m-%d %H:%M:%S %z")
            return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            logging.error("Invalid datetime format: %s", dt)
            # Handle the error appropriately (e.g., return None or a default value)
            return None


    @staticmethod
    def init_db():
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs
                (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name TEXT NOT NULL,
                    status INT NOT NULL,
                    log TEXT,
                    format TEXT,
                    last_update DATETIME DEFAULT CURRENT_TIMESTAMP,
                    type INT NOT NULL,
                    url TEXT,
                    pid INT
                );
            """
        )
        conn.commit()
        conn.close()

    def __init__(self, readonly=True):
        self.readonly = readonly
        self.conn = get_db_connection()
        if readonly:
            self.conn.autocommit = False
        else:
            self.conn.autocommit = True
        logging.info("Initialized JobsDB with readonly=%s", readonly)

    def close(self):
        logging.info("Closing database connection")
        try:
            self.conn.close()
        except Exception as e:
            logging.error("Error closing database connection: %s", e)
            raise
    def insert_job(self, job):
        logging.info("Inserting job: %s", job.name)
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                INSERT INTO jobs
                    (name, status, log, format, type, url, pid)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    job.name,
                    job.status,
                    job.log,
                    job.format,
                    job.type,
                    "\n".join(job.url),
                    job.pid,
                ),
            )
            job.id = cursor.lastrowid
            self.conn.commit()
            logging.info("Job inserted with ID: %s", job.id)
        except Exception as e:
            logging.error("Error inserting job: %s", e)
            raise

    def update_job(self, job):
        logging.info("Updating job: %s", job.id)
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                UPDATE jobs
                SET status = %s, log = %s, last_update = NOW()
                WHERE id = %s;
                """,
                (job.status, job.log, job.id),
            )
            self.conn.commit()
            logging.info("Job updated: %s", job.id)
        except Exception as e:
            logging.error("Error updating job: %s", e)
            raise

        def set_job_status(self, job_id, status):
            logging.info("Setting job status for job_id=%s", job_id)
            try:
                cursor = self.conn.cursor()
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = %s, last_update = NOW()
                    WHERE id = %s;
                    """,
                    (status, job_id),
                )
                self.conn.commit()
                logging.info("Job status updated for job_id=%s", job_id)
            except Exception as e:
                logging.error("Error setting job status: %s", e)
                raise
    
        def set_job_pid(self, job_id, pid):
            logging.info("Setting job PID for job_id=%s", job_id)
            try:
                cursor = self.conn.cursor()
                cursor.execute(
                    """
                    UPDATE jobs
                    SET pid = %s, last_update = NOW()
                    WHERE id = %s;
                    """,
                    (pid, job_id),
                )
                self.conn.commit()
                logging.info("Job PID set for job_id=%s", job_id)
            except Exception as e:
                logging.error("Error setting job PID: %s", e)
                raise
    
        def set_job_log(self, job_id, log):
            logging.info("Setting job log for job_id=%s", job_id)
            try:
                truncated_log = log[-2500:] if len(log) > 2500 else log
                cursor = self.conn.cursor()
                cursor.execute(
                    """
                    UPDATE jobs
                    SET log = %s, last_update = NOW()
                    WHERE id = %s;
                    """,
                    (truncated_log, job_id),
                )
                self.conn.commit()
                logging.info("Job log set for job_id=%s", job_id)
            except Exception as e:
                logging.error("Error setting job log: %s", e)
                raise
    #    def set_job_log(self, job_id, log):
    ##        cursor = self.conn.cursor()
     #       cursor.execute(
    #            """
    #            UPDATE jobs
    #            SET log = %s, last_update = NOW()
    #            WHERE id = %s;
    #            """,
    #            (log, job_id)  # Use the full log directly
    #        )
    #        self.conn.commit()
        
    
    
    
    def set_job_name(self, job_id, name):
        logging.info("Setting job name for job_id=%s", job_id)
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                UPDATE jobs
                SET name = %s, last_update = NOW()
                WHERE id = %s;
                """,
                (name, job_id),
            )
            self.conn.commit()
            logging.info("Job name set for job_id=%s", job_id)
        except Exception as e:
            logging.error("Error setting job name: %s", e)
            raise

    def purge_jobs(self):
        logging.info("Purging all jobs")
        try:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM jobs;")
            self.conn.commit()
            cursor.execute("VACUUM")
            logging.info("All jobs purged")
        except Exception as e:
            logging.error("Error purging jobs: %s", e)
            raise

    def delete_job(self, job_id):
        logging.info("Deleting job with job_id=%s", job_id)
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "DELETE FROM jobs WHERE id = %s AND (status = %s OR status = %s);",
                (job_id, Job.ABORTED, Job.FAILED),
            )
            self.conn.commit()
            cursor.execute("VACUUM")
            logging.info("Job deleted with job_id=%s", job_id)
        except Exception as e:
            logging.error("Error deleting job: %s", e)
            raise

    def clean_old_jobs(self, limit=10):
        logging.info("Cleaning old jobs, limit=%s", limit)
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT last_update
                FROM jobs
                ORDER BY last_update DESC
                LIMIT %s;
                """,
                (limit,)
            )
            rows = list(cursor.fetchall())
            if len(rows) > 0:
                cursor.execute(
                    "DELETE FROM jobs WHERE last_update < %s AND status != %s and status != %s;",
                    (rows[-1][0], Job.PENDING, Job.RUNNING),
                )
            self.conn.commit()
            logging.info("Old jobs cleaned")
        except Exception as e:
            logging.error("Error cleaning old jobs: %s", e)
            raise

    def get_job_by_id(self, job_id):
        logging.info("Getting job by job_id=%s", job_id)
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT
                    id, name, status, log, last_update, format, type, url, pid
                FROM
                    jobs
                WHERE id = %s;
                """,
                (job_id,)
            )
            row = cursor.fetchone()
            if not row:
                logging.info("No job found with job_id=%s", job_id)
                return
            job = {
                "id": row[0],
                "name": row[1],
                "status": STATUS_NAME[row[2]],
                "log": row[3],
                "format": row[4],
                "last_update": JobsDB.convert_datetime_to_tz(row[5]),
                "type": row[6],
                "urls": row[7].split("\n"),
                "pid": row[8],
            }
            logging.info("Job retrieved: %s", job)
            return job
        except Exception as e:
            logging.error("Error getting job by ID: %s", e)
            raise

    def get_all(self, limit=50):
        logging.info("Getting all jobs, limit=%s", limit)
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT
                    id, name, status, log, last_update, format, type, url, pid
                FROM
                    jobs
                ORDER BY last_update DESC LIMIT %s;
                """,
                (limit,)
            )
            rows = cursor.fetchall()
            jobs = [
                {
                    "id": row[0],
                    "name": row[1],
                    "status": STATUS_NAME[row[2]],
                    "log": row[3],
                    "format": row[4],
                    "last_update": JobsDB.convert_datetime_to_tz(row[5]),
                    "type": row[6],
                    "urls": row[7].split("\n"),
                    "pid": row[8],
                }
                for row in rows
            ]
            logging.info("Retrieved all jobs")
            return jobs
        except Exception as e:
            logging.error("Error getting all jobs: %s", e)
            raise

    def get_jobs(self, limit=50):
        logging.info("Getting jobs, limit=%s", limit)
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT
                    id, name, status, last_update, format, type, url, pid
                FROM
                    jobs
                ORDER BY last_update DESC LIMIT %s;
                """,
                (limit,)
            )
            rows = cursor.fetchall()
            jobs = [
                {
                    "id": row[0],
                    "name": row[1],
                    "status": STATUS_NAME[row[2]],
                    "format": row[3],
                    "last_update": JobsDB.convert_datetime_to_tz(row[4]),
                    "type": row[5],
                    "urls": row[6].split("\n"),
                    "pid": row[7],
                }
                for row in rows
            ]
            logging.info("Retrieved jobs")
            return jobs
        except Exception as e:
            logging.error("Error getting jobs: %s", e)
            raise
   
