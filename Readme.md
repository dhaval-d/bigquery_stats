<h2>Purpose</h2>
This application is a small tool to gather storage statistics about tables and datasets
for your BigQuery GCP project.

The application uses \_\_TABLES\_\_ table to gather storage statistics about
each table in a dataset. Application loops through all datasets one by one.

PLEASE NOTE: This application stores all of these stats in a dataset named utils and
a table named daily_storage_stats. If they don't exist in a given project, application
creates them for you.


<h2>Steps to run this application</h2>
<ol>
<li>Create a service account with following permissions:
    <ul>
        <li>BigQuery Data Editor</li>
        <li>BigQuery Data Reader</li>
        <li>BigQuery Job user </li>
    </ul>
</li>
<li>Create and download a service account key file in a JSON format</li>
<li>Download a source code to your GCE instance or a local machine and go to root of the directory.</li>
<li>Run following steps to create and activate a virtual environment<br>
<code>virtualenv venv</code> <br>
<code>source venv/bin/activate</code>
</li>
<li>Now install dependencies<br>
<code>
pip install -r requirements.txt</code>
</li>
<li>Once these requirements are installed, use following command to run your application.<br>
<code>
python main.py --project_id=google.com:testdhaval --service_account_file=bqwriter.json
</code>
<br>
--project_id = project id for which we are capturing storage size for each table
--service_account_file = location of the key file for your service account.
</li>    
<li>
Once this script finishes, you can run following query:<br>
<code>
select * from `{PROJECT_ID}.utils.daily_storage_stats`
</code>
<br>
{PROJECT_ID} - project_id you provided as an argument when you executed your application.
</li>
</ol>


<b>****Please note:*****</b> You can schedule this application to run on a nightly basis in a cron job to get a daily snapshot of your storage usage for every table and build dashboard to analyze this information.
