from flask import Flask, render_template, request, redirect, url_for, flash
from markupsafe import Markup
import snowflake.connector
import logging
import time
from urllib.parse import unquote
from snowflake.connector.errors import OperationalError

app = Flask(__name__)
app.secret_key = 'JvpeKVgeYzERzscpnqGLfy8a'

# Enable logging
logging.basicConfig(level=logging.DEBUG)

# Snowflake connection parameters
connection_parameters = {
    'user': 'dineshstrsi',
    'password': 'Ramjob1985@',
    'account': 'udb96672',
    'warehouse': 'COMPUTE_WH',
    'database': 'STRATEGIC_RESOURCE',
    'schema': 'PUBLIC'
}

def get_snowflake_connection(retries=3, wait=5):
    attempt = 0
    while attempt < retries:
        try:
            logging.debug("Attempting to connect to Snowflake with the following details:")
            logging.debug(f"user: {connection_parameters['user']}")
            logging.debug(f"password: {'<hidden>'}")
            logging.debug(f"account: {connection_parameters['account']}")
            logging.debug(f"role: {connection_parameters.get('role', 'No role provided')}")
            logging.debug(f"warehouse: {connection_parameters['warehouse']}")
            logging.debug(f"database: {connection_parameters['database']}")
            logging.debug(f"schema: {connection_parameters['schema']}")

            conn = snowflake.connector.connect(
                **connection_parameters,
                ocsp_fail_open=True  # Adjust OCSP mode if necessary
            )
            logging.debug("Snowflake connection established successfully.")
            return conn
        except OperationalError as e:
            logging.error(f"Error connecting to Snowflake: {e}")
            attempt += 1
            logging.debug(f"Retrying connection in {wait} seconds...")
            time.sleep(wait)
    logging.error("All attempts to connect to Snowflake failed.")
    return None

@app.route('/')
def index():
    conn = get_snowflake_connection()
    if not conn:
        flash("Unable to connect to the database. Please check your credentials.", "error")
        return render_template('index.html', technologies=[])

    try:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT TECHNOLOGY FROM CANDIDATES')
        technologies = cursor.fetchall()
        cursor.close()
        conn.close()
        return render_template('index.html', technologies=technologies)
    except Exception as e:
        logging.error(f"Error fetching technologies: {e}")
        flash("Error fetching data from the database.", "error")
        return render_template('index.html', technologies=[])

@app.route('/technology/<technology>')
def technology(technology):
    conn = get_snowflake_connection()
    if not conn:
        flash("Unable to connect to the database. Please check your credentials.", "error")
        return render_template('technology.html', technology=technology, candidates=[])
    
    try:
        cursor = conn.cursor()
        logging.debug(f'Executing query: SELECT CANDIDATE_FULL_LEGAL_NAME FROM CANDIDATES WHERE TECHNOLOGY = %s', (technology,))
        cursor.execute("SELECT CANDIDATE_FULL_LEGAL_NAME FROM CANDIDATES WHERE TECHNOLOGY = %s", (technology,))
        candidates = cursor.fetchall()
        cursor.close()
        conn.close()
        return render_template('technology.html', technology=technology, candidates=candidates)
    except Exception as e:
        logging.error(f"Error fetching data for technology {technology}: {e}")
        flash(f"Error fetching data for technology {technology} from the database.", "error")
        return render_template('technology.html', technology=technology, candidates=[])

@app.route('/candidate/<candidate>')
def candidate(candidate):
    conn = get_snowflake_connection()
    if not conn:
        flash("Unable to connect to the database. Please check your credentials.", "error")
        return render_template('candidate.html', candidate=candidate, details={}, technology="")
    
    try:
        cursor = conn.cursor()
        candidate = unquote(candidate)  # Decode URL-encoded candidate name
        logging.debug(f'Executing query: SELECT * FROM CANDIDATES WHERE CANDIDATE_FULL_LEGAL_NAME = %s', (candidate,))
        cursor.execute(
            """
            SELECT
                CANDIDATE_FULL_LEGAL_NAME,
                TECHNOLOGY,
                SKILL_SET,
                RELOCATION,
                CURRENT_LOCATION,
                BDM,
                LINKEDIN_ID
            FROM
                CANDIDATES
            WHERE
                CANDIDATE_FULL_LEGAL_NAME = %s
            """,
            (candidate,)
        )
        details = cursor.fetchone()
        logging.debug(f"Details fetched for candidate {candidate}: {details}")
        if details:
            details_dict = {
                'CANDIDATE_FULL_LEGAL_NAME': details[0],
                'TECHNOLOGY': details[1],
                'SKILL_SET': details[2],
                'RELOCATION': details[3],
                'CURRENT_LOCATION': details[4],
                'BDM': details[5],
                'LINKEDIN_ID': details[6]
            }
            technology = details[1]  # Assuming the second column is the technology
        else:
            details_dict = {}
            technology = ""
        cursor.close()
        conn.close()
        return render_template('candidate.html', candidate=candidate, details=details_dict, technology=technology)
    except Exception as e:
        logging.error(f"Error fetching data for candidate {candidate}: {e}")
        flash(f"Error fetching data for candidate {candidate} from the database.", "error")
        return render_template('candidate.html', candidate=candidate, details={}, technology="")

if __name__ == '__main__':
    print("Starting Flask app")
    app.run(debug=True)
