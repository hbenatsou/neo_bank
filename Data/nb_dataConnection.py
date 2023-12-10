###############################################################
############ Data BigQuery Connexion ##########################
###############################################################

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq  as pd_gbq

class BigQueryConnection:
  def __init__(self, credentialPath, project_id):
    self.project_id = project_id
    self.credentials = service_account.Credentials.from_service_account_file(credentialPath)

  def getClient(self):
    return bigquery.Client(credentials= self.credentials,project= self.project_id)
  
  def executeQuery(self,strQuery,use_legacy_sql=False):
    query_job = self.getClient().query(strQuery)
    return query_job.result()
  
  def get_dataframe(self,strQuery):
    return pd.read_gbq(query=strQuery, project_id=self.project_id)
  
  def saveDataframe(self,df,table_id):
    pd_gbq.to_gbq(df, table_id, self.project_id,credentials = self.credentials,if_exists="replace")

  def to_datatable(self,df,table_id):
    pd_gbq.to_gbq(df, table_id, self.project_id,credentials = self.credentials,if_exists="replace")
