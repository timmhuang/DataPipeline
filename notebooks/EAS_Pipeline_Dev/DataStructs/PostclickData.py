# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("debug", "", "debug")
DEBUG_ON = dbutils.widgets.get("debug")

# COMMAND ----------

# MAGIC %run ../Constants

# COMMAND ----------

# MAGIC %run ../Components/OutputWriter

# COMMAND ----------

class PostclickData:
  """
  @param src_data: the input data struct for postclick transformation (SourceData)
  @param drop_list: list of columns to drop from final table (str[])
  """
  def __init__(self, name, src_data, drop_list=None):
    self.src_data = src_data
    self.drop_list = drop_list
    self.table_name = name
    self.data_set = None
    self.writer = OutputWriter(self.table_name)
    self.__process_data()
  
  def __process_data(self):
    mkts = ""
    for field in self.src_data.mkt_fields:
      mkts = "".join([mkts, "a2.%s," % MKT_READABLE_NAMES[field]])
      
    query = """
    SELECT DISTINCT a1.*, %s a2.locid_parm, a2.campaign 
    FROM %s a1 INNER JOIN %s a2 
    ON a1.f_event_guid_userid1 = a2.f_event_guid_userid1 AND a1.f_date=a2.f_date AND 
    a1.f_event_guid_sessionid = a2.f_event_guid_sessionid AND a1.locationid = a2.locationid
    """ % (mkts, self.src_data.raw_table_name, self.src_data.table_name)
    if DEBUG_ON == 'true':
      print(query)
    
    intermediary_table = (sqlContext.sql(query).withColumn("locationid", F.udf(lambda locid, locparam: locparam if (locid != locparam and locparam != '') else locid)(F.col('locationid'), F.col('locid_parm'))))  
    if self.drop_list:
      intermediary_table = intermediary_table.select(
        [column for column in intermediary_table.columns if column not in self.drop_list]
      )
    name = "%s_intermediary" % self.table_name
    intermediary_table.registerTempTable(name)
    
    print("processing postclick output table...")
    final_table = sqlContext.sql("""
      SELECT DISTINCT a1.*, a2.dealername 
      FROM %s a1 LEFT JOIN dlrdata_t a2 
      ON a1.locationid = a2.f_locationid
    """ % name)
    final_table.persist()
    final_table.createOrReplaceTempView(self.table_name)
    self.data_set = final_table