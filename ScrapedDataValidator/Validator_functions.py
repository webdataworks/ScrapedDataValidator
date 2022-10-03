from pyspark.sql.functions import col,isnan,when,count
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("learning-dataframe").getOrCreate()


def input_csv_file(file_name:str):
    df = spark.read.option('header',"true").csv(file_name, inferSchema = True)
    return df


def input_txt_file(file:str):
    input_list = spark.read.text(file).collect()
    #file2 = spark.read.text(detail_file).collect()
    
    return input_list


def missing_attribute_test(df:str, columns:list):
    
    #columns = ["Product_Code","Product_Name", "Search_Term", "rank","shallow_url","DetailURL"]
    
    return {col:("Missing" if df.filter(df[col].isNull()).count()>0 else "No Missing") for col in columns}

def non_empty_results_for_each_search_term_test(df:str):
    
    result = dict()
    term_list = df.select("Search_Term").distinct().collect()
    for col in term_list:
        col = col["Search_Term"]
        if int(df.where(df["Search_Term"] == col).count()) == 0:
            result[col] = "Empty"
        else:
            result[col] = "Non-Empty"

    return result


def search_terms_not_in_csv_test(df:str, search_list:list):
# file contains the search terms
# need to match the search terms in the file with the search terms in the csv
    result = dict()
    for search_term in search_list:
        search_term_from_list = search_term["value"]
        
        # find this search term in csv
# df_filtered.where(df_filtered["Search_Term"] == term)

        if df.where(df["Search_Term"] == search_term_from_list):
            result[search_term_from_list] = "Present"
        else:
            result[search_term_from_list] = "Absent"
    
    return result

# are all the products listed included in the scrape?
def products_not_listed_in_scrape_test(df:str, url_list:list):
    
    result = dict()
    for url in url_list:
        url_from_list = url["value"]
        
        if df.where(df["URL"] == url_from_list):
            result[url_from_list] = "Present"
        else:
            result[url_from_list] = "Absent"
            
    return result

def rank_ordering_test(df:str):
    
    result = dict()
    term_list = df.select("Search_Term").distinct().collect()
    for term in term_list:
        term = term["Search_Term"]
        infered_sum = int(df.where(df["Search_Term"] == term).select("rank").groupBy().sum().collect()[0][0])
        n = int(df.where(df["Search_Term"] == term).count())
        GT_sum = (n)*(n+1)/2
        
        if infered_sum == int(GT_sum):
            result[term] = "ordered" 
        else:
            result[term] = "un-ordered" 
    
    return result




