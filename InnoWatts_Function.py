
def Init( df1, df2):
    '''
    For description of spark data frames
    :param df1: spark data frame 1
    :param df2: spark data frame 2
    '''
    parent_col = enumerate(df1.columns)
    child_col = enumerate(df2.columns)
    print(list(parent_col))
    print(list(child_col))

def SQL( function, column, alias):
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
                    z.state'''.format(function, column, alias)
    return _sql


def OutPut(df):
    '''
    export the data with desired file name at the requested directory and partitioned by year of certified
    :param df: dataframe
    file_path: the output file path
    type: it could be either parquet, csv or json
    '''

    file_path=input('Please specify the folder: ')
    type=input('Please select either parquet, csv or json: ')
    if len(file_path) == 0:
        print('the file name has not been specified: ')
    elif type != 'parquet' and type != 'json' and type != 'csv':
        print('invalid file type: ')
    else:
        df.write.format(type)\
                .mode('overwrite')\
                .save(file_path)




