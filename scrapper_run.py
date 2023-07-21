from selenium import webdriver
from selenium import __version__
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.remote import remote_connection
import pygsheets
from time import sleep
from datetime import datetime
from base64 import b64encode
from flask import Flask,jsonify,current_app
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
from selenium.webdriver.common.by import By
import psycopg2
import os
import platform
import pandas as pd
import traceback
import google.auth.transport.requests
import google.oauth2.id_token

app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))


if os.environ.get('SELENIUM_URL') is not None:
    selenium_url = os.environ.get('SELENIUM_URL')
else:
    raise Exception('No remote Selenium webdriver provided in the environment.')

# Overwriting the RemoteConnection class in order to authenticate with the Selenium Webdriver in Cloud Run.
class RemoteConnectionV2(remote_connection.RemoteConnection):
    @classmethod
    def set_remote_connection_authentication_headers(self):
        # Environment variable: identity token -- this can be set locally for debugging purposes.
        if os.environ.get('IDENTITY_TOKEN') is not None:
            print('[Authentication] An identity token was found in the environment. Using it.')
            identity_token = os.environ.get('IDENTITY_TOKEN')
        else:
            print('[Authentication] No identity token was found in the environment. Requesting a new one.')
            auth_req = google.auth.transport.requests.Request()
            identity_token = google.oauth2.id_token.fetch_id_token(auth_req, selenium_url)
        self._auth_header = {'Authorization': 'Bearer %s' % identity_token}
    
    @classmethod
    def get_remote_connection_headers(self, cls, parsed_url, keep_alive=False):
        """
        Get headers for remote request -- an update of Selenium's RemoteConnection to include an Authentication header.
        :Args:
         - parsed_url - The parsed url
         - keep_alive (Boolean) - Is this a keep-alive connection (default: False)
        """

        system = platform.system().lower()
        if system == "darwin": 
            system = "mac"

        default_headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json;charset=UTF-8',
            'User-Agent': 'selenium/{} (python {})'.format(__version__, system)
        }

        headers = {**default_headers, **self._auth_header}
        if 'Authorization' not in headers:
            if parsed_url.username:
                base64string = b64encode('{0.username}:{0.password}'.format(parsed_url).encode())
                headers.update({
                    'Authorization': 'Basic {}'.format(base64string.decode())
                })

        if keep_alive:
            headers.update({
                'Connection': 'keep-alive'
            })

        return headers

def write_gsheet (get_tbl_dict, client_name):
    #nth-boulder-368917-864bca93c399.json
    client = pygsheets.authorize(service_account_file="cavewebworks-scraping-65b3c290b16b.json")
    sheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1aQJML88bwlE_aS5MIkBGYrzs0Vgnw6ZJToIA8MXwpqI/edit#gid=0')
    wks = sheet.worksheet_by_title(client_name)
    wks.clear()
    df = pd.DataFrame(get_tbl_dict.items(), columns=['metric','value'])
    wks.set_dataframe(df, start='A1', end=None, dimension='ROWS', overwrite=True)
    wks.update_value('C1', 'Completedg')
    print("%s completed" %(client_name))
    
    #Traffic
   
    #adding impressions and conversions
def final_dict( get_tbl, impressions,client_name):
    final_dict={}
    gt=list(map(lambda x: convert_dict(x), get_tbl))
    imp=list(map(lambda x: convert_dict(x), impressions))
 
    for i in gt:
        final_dict.update(i)
    for i in imp:
        final_dict.update(i)  
    write_gsheet(final_dict, client_name)
   


def get_table(driver, xpath):
    tbl=driver.find_element("xpath", xpath)
    lst2=[]
    for a in tbl.text.split("\n"):
        if "." not in a:
            lst2.append(a)
        else:
            continue
    return lst2[2:] #removing header.

def convert_dict(a):
    print(a)
    it = iter(a)
    res_dct = dict(zip(it, it))
    for k,v in list(res_dct.items()):
        if v[0].isalpha() :
            del res_dct[k]
    #removing ,
    res_dct=dict(map(lambda x: (x[0],float(x[1].replace(",","").replace(" ","").replace("  ","").replace("$",""))), res_dct.items()))
    return res_dct

def get_impressions(driver,xpath):
    imp=driver.find_element("xpath", xpath)
    a=imp.text.split("\n")
    return a

def read_json(chrome_driver):
    app.logger.info("read json started")
    f=open("val.json", 'r', encoding="utf-8")
    data = json.loads(f.read())
    print("I am here")
    i=0
    for i in data["client"]:
        client_name=i["Name"]
        print(client_name)
        get_tbl=i["gettable"]
        impressions=i["impressions"]
        d_link=i["d_link"]
        print(d_link)
        impression=[]
        gettable=[]
        chrome_driver.get(d_link)    
        if get_tbl['is_available']=="1":
            xpaths=list(get_tbl.values())
            element_present = EC.presence_of_element_located((By.XPATH, xpaths[1]))
            WebDriverWait(chrome_driver, 10).until(element_present)
            for path in xpaths[1:]:
                gettable.append(get_table(chrome_driver, path.replace("\\","")))

        if impressions['is_available']=="1":
            xpaths=list(impressions.values())
            for path in xpaths[1:]:
                impression.append(get_impressions(chrome_driver, path.replace("\\","")))
        if(i==0):
            break
    chrome_driver.close()
    final_dict(gettable,impression,client_name)

@app.route("/")
def scrape():
    selenium_connection = RemoteConnectionV2(selenium_url, keep_alive = True)
    selenium_connection.set_remote_connection_authentication_headers()
    chrome_driver = webdriver.Remote(selenium_connection, DesiredCapabilities.CHROME.copy())
    try:
        read_json(chrome_driver)
        return jsonify({'success': True, "result": "completed"})
        
    
    except Exception as e:
        print (traceback.format_exc())
        return jsonify({'success': False, 'msg': str(e)})
    
   

    

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
