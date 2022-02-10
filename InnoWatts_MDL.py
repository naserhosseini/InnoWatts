from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
import InnoWatts_Function as F

conf=SparkConf().setAppName('InnoWatts').setMaster('local')
sc=SparkContext(conf=conf)

spark=SparkSession(sc)
#Import Home Health Service data
HHS=spark.read.csv('HH_Provider_Jan2022.csv',header=True)
HHS.createOrReplaceTempView('Main')

#Import ZipCode data
ZC=spark.read.csv('HH_Zip_Jan2022.csv',header=True)
ZC.createOrReplaceTempView('ZipCode')

C_Spark=F.Init(HHS,ZC)
col=int(input('Please enter the index column of Parent table: '))
print('You have selected column: {}'.format(HHS.columns[col]))
fun=input('Please enter the function: ')
alias_name=input('Please enter the alias name: ')
sql_state=F.SQL(fun,'Quality_of_patient_care_star_rating',alias_name)

df_joint=HHS.join(ZC,HHS.ZIP==ZC.ZIPCode,'inner')
Results=spark.sql(sql_state)
Results.show()

HHS_Count = df_joint.orderBy('state')\
                    .groupBy('state')\
                    .agg({'Date_Certified':'min','Date_Certified':'max','Provider_Name':'count'})\
                    .withColumnRenamed('MIN(Date_Certified)','From')\
                    .withColumnRenamed('MAX(Date_Certified)','To')\
                    .withColumnRenamed('COUNT(Provider_name)','Count of Providers')
HHS_Count.show()

F.OutPut(df_joint)