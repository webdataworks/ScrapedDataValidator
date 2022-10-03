
from Validator_functions import *

#from Json_handler import json_extract

# TODO -> separate the json handling from run_fns

def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values
    
# dictionary to reduce if/else statements when parsing json
run_logic = {
    "missing_attribute_test":missing_attribute_test,
    "non_empty_results_for_each_search_term_test":non_empty_results_for_each_search_term_test,
    "search_terms_not_in_csv_test":search_terms_not_in_csv_test,
    "rank_ordering_test":rank_ordering_test,
    "products_not_listed_in_scrape_test":products_not_listed_in_scrape_test,
    "products_not_listed_in_scrape_test":products_not_listed_in_scrape_test
}


def run_fns(file):
    
    run_id = json_extract(file, "run_id")[0]
    search_csv = json_extract(file, "search_csv")[0]
    detail_csv = json_extract(file, "detail_csv")[0]
    search_txt = json_extract(file, "search_txt")[0]
    detail_txt = json_extract(file, "detail_txt")[0]
        
    for search_test in file["tests"]["search_test"]:
        
        search_df = input_csv_file(search_csv)
        search_list = input_txt_file(search_txt)
        
        action = run_logic.get(search_test)
        print(action)
        
        if action == missing_attribute_test and file["tests"]["search_test"]["missing_attribute_test"]["status"] == 1:
            cols = file["tests"]["search_test"]["missing_attribute_test"]["columns"]
            search_missing_attribute = action(df = search_df, columns = cols)
        
        if action == non_empty_results_for_each_search_term_test and file["tests"]["search_test"]["non_empty_results_for_each_search_term_test"]["status"] == 1:
            search_non_empty_attributes = action(df = search_df)
            
        if action == search_terms_not_in_csv_test and file["tests"]["search_test"]["search_terms_not_in_csv_test"]["status"] == 1:
            search_terms_not_in_csv = action(df = search_df, search_list = search_list)
    
        if action == rank_ordering_test and file["tests"]["search_test"]["rank_ordering_test"]["status"] == 1:
            search_rank_order = action(df = search_df)
    
    for detail_test in file["tests"]["detail_test"]:
        
        detail_df = input_csv_file(detail_csv)
        url_list = input_txt_file(detail_txt)
        
        action = run_logic.get(detail_test)
        print(action)
        
        if action == missing_attribute_test and file["tests"]["detail_test"]["missing_attribute_test"]["status"] == 1:
            cols_2 = file["tests"]["detail_test"]["missing_attribute_test"]["columns"]
            detail_missing_attribute = action(df = detail_df, columns = cols_2)
        
        if action == products_not_listed_in_scrape_test and file["tests"]["detail_test"]["products_not_listed_in_scrape_test"]["status"] == 1:
            detail_products_not_listed_in_scrape = action(df = detail_df, url_list = url_list)
            
    output = {"run_id": f"{run_id}",
        "search_csv": f"{search_csv}",
        "detail_csv": f"{detail_csv}",
        "search_txt": f"{search_txt}",
        "detail_txt": f"{detail_txt}",
        "search_missing_attributes": search_missing_attribute,
        "search_non-empty_attributes":search_non_empty_attributes,
        "search_terms_not_in_csv":search_terms_not_in_csv,
        "search_rank_order":search_rank_order,
        "detail_missing_attribute": detail_missing_attribute,
        "detail_products_not_listed_in_scrape": detail_products_not_listed_in_scrape
    }
    
    return output