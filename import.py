import click
from elasticsearch import Elasticsearch
from dotenv import load_dotenv, find_dotenv 
from os import makedirs, environ, path
from field_information import name_to_field_and_key as aggregation_map

import pandas as pd
import numpy as np

# READ .env file if any
load_dotenv(find_dotenv())
ELASTICSEARCH_URL = environ.get("ELASTICSEARCH_URL", "http://localhost:9200/")
ELASTICSEARCH_USER = environ.get("ELASTICSEARCH_USER","")
ELASTICSEARCH_PASS = environ.get("ELASTICSEARCH_PASS","")

client = Elasticsearch(ELASTICSEARCH_URL, basic_auth=(ELASTICSEARCH_USER,ELASTICSEARCH_PASS))

@click.command()
@click.argument("indice", required=True)
@click.option("-f","--file",required=True, type=click.Path(exists=True))
def main(indice,file):
    """
        This tool will exports INDICE into .xlsx files under the INDICE folder for each field in the INDICE.
        Each .xlsx file has the following columns:

         | Correção | ID | Original | Atual | Secção |
         |----------|----|----------|-------|--------|
    """
    known_props = list(aggregation_map.keys())
    xls = pd.ExcelFile(file)
    props = list(p for p in xls.sheet_names if p in known_props)
    print("checking for updates on the following props", props)
    
    for prop_name in props:
        print(prop_name)
        df = pd.read_excel(xls, prop_name, keep_default_na=False)
        if not ('Correção' in df.columns and 'Atual' in df.columns and 'Secção' in df.columns):
            print("Expected excel to have all the folloing columns: Correção, Atual e Secção")
            exit(1)
        if 'ID' in df.columns:
            for _index, row in df.query("Correção != ''").iterrows():
                n = update_uuid(indice, prop_name, row["ID"], row["Atual"], row["Correção"])
                print(row["ID"], row['Atual'],"=>",row['Correção'],n)
        
        else: 
            for _index, row in df.query("Correção != ''").iterrows():
                # update by agregations
                n = update_all(indice, prop_name, row['Atual'],row['Correção'],row['Secção'] if row['Secção'] != '*' else None)
                print(row['Atual'],"=>",row['Correção'],n)

def update_value(indice, hit, prop_name, old_value, new_value):
    if old_value == new_value:
        return 0
    curr_value_or_array = hit["_source"][prop_name]
    if isinstance(curr_value_or_array, list):
        curr_array = curr_value_or_array[:]
        using = False
        if old_value == "":
            curr_array.append(new_value)
            using = True
        else:
            for idx, value in enumerate(curr_array):
                if value == old_value:
                    curr_array[idx] = new_value
                    using=True
                if value == "":
                    curr_array[idx] = "«sem valor»"
                    using = True
        if using:
            # cleanup
            curr_array = list( v for v in curr_array if v != "«sem valor»" or v != "")
            if len(curr_array) == 0:
                curr_array = [f"«sem valor»"]
            client.update(index=indice, id=hit["_id"], doc={prop_name: curr_array})
            return 1
    elif isinstance(curr_value_or_array, str):
        curr_value = curr_value_or_array
        if curr_value == old_value:
            client.update(index=indice, id=hit["_id"], doc={prop_name: new_value if new_value != '' else f"«sem valor»"})
            return 1
    return 0


def update_uuid(indice, prop_name, uuid, old_value, new_value):
    if old_value == new_value: 
        return 0
    n=0
    must = [{"term": {"UUID": uuid}}]
    r = client.search(index=indice, source=[prop_name], scroll="1m", query={"bool": {"must": must}})
    while len(r["hits"]["hits"]) > 0:
        for hit in r["hits"]["hits"]:
            n+=update_value(indice, hit, prop_name, old_value, new_value)

        r = client.scroll(scroll='1m', scroll_id=r.get("_scroll_id"))
    client.clear_scroll(scroll_id=r.get("_scroll_id"))
    return n

def update_all(indice, prop_name, old_value, new_value, section):
    if old_value == new_value: 
        return 0
    n = 0
    must = [{"term": {aggregation_map[prop_name][0]: old_value}}]
    if section:
        must.append({"term": {aggregation_map['Secção'][0]: section}})

    r = client.search(index=indice, source=[prop_name,"UUID"], scroll="2m", query={"bool": {"must": must}})
    while len(r["hits"]["hits"]) > 0:
        for hit in r["hits"]["hits"]:
            n+=update_value(indice, hit, prop_name, old_value, new_value)

        r = client.scroll(scroll='1m', scroll_id=r.get("_scroll_id"))
    
    client.clear_scroll(scroll_id=r.get("_scroll_id"))
    return n


if __name__ == "__main__":
    main()
