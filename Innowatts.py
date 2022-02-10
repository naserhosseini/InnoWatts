from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf

class SparkSql:
    def __init__(self,df1,df2):
        '''
        For description of spark data frames
        :param df1: spark data frame 1
        :param df2: spark data frame 2
        '''
        parent_col= enumerate(df1.columns)
        child_col = enumerate(df2.columns)
        print(list(parent_col))
        print(list(child_col))

    def SQL(self, function, column, alias):
        '''
        The function will return the proper SQL statement for jointing to spark data frames on zipcode,
        group by state
        :param function: the desired aggregate function
        :param column: the desired column
        :param alias: the desired alias name for aggregated column
        :return: the sql statement
        '''

        _sql = '''SELECT 
                        z.state,
                        {}(m.{}) as {}
                    FROM
                        Main as m
                    INNER JOIN
                        ZipCode as z
                    ON
                        m.ZIP=z.ZIPCode
                    GROUP BY
                        z.state'''.format(function,column,alias)
        return _sql

    def OutPut(self,df,file_name,type='parquet'):
        '''
        export the data with desired file name at the same directory
        :param df: dataframe
        :param file_name: the output file name
        :param type: it could be either parquet, csv or json
        '''
        if len(file_name)==0:
            print('the file name has not been specified')
        elif type!='parquet' and type!='json' and type!='csv':
            print('invalid file type')
        else:
            df.write.format(type).save(file_name)

conf=SparkConf().setAppName('InnoWatts').setMaster('local')
sc=SparkContext(conf=conf)

spark=SparkSession(sc)
#Import Home Health Service data
HHS=spark.read.csv('HH_Provider_Jan2022.csv',header=True)
HHS.createOrReplaceTempView('Main')

#Import ZipCode data
ZC=spark.read.csv('HH_Zip_Jan2022.csv',header=True)
ZC.createOrReplaceTempView('ZipCode')

C_Spark=SparkSql(HHS,ZC)
col=int(input('Please enter the index column of Parent table:'))
fun=input('Please enter the function:')
alias_name=input('Please enter the alias name:')
print(HHS.columns[col])
sql_state=C_Spark.SQL(fun,'Quality_of_patient_care_star_rating',alias_name)

s=spark.sql(sql_state)
s.show()
#C_Spark.OutPut(s,'test.csv',type='csv')
