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

def update_uuid(indice, prop_name, uuid, old_value, new_value):
    i=0
    n=0
    must = [{"term": {"UUID": uuid}}]
    r = client.search(index=indice, source=[prop_name], scroll="2m", query={"bool": {"must": must}})
    while i < r["hits"]["total"]["value"]:
        for hit in r["hits"]["hits"]:
            curr_value = hit["_source"][prop_name]
            if isinstance(curr_value, list):
                using=False
                if old_value == "":
                    curr_value.append(new_value)
                    using=True
                for idx, curr_old_value in enumerate(curr_value):
                    if curr_old_value == old_value:
                        curr_value[idx] = new_value
                        using=True
                new_list = [v for v in curr_value if v != '']
                if len(new_list) == 0:
                    new_list = [f"sem {prop_name}"]
                if using:
                    client.update(index=indice, id=hit["_id"], doc={prop_name: new_list})
                    n+=1
                else:
                    print(f"WARNING ON: {prop_name} {old_value} => {new_value} uuid {uuid}")
                    print(f"            curr_value was array without {old_value}")
                    print(f"            update ignored")

            elif isinstance(curr_value, str):
                client.update(index=indice, id=hit["_id"], doc={prop_name: new_value if new_value != '' else f"sem {prop_name}"})
                n+=1
            else:
                # value was null
                client.update(index=indice, id=hit["_id"], doc={prop_name: [new_value if new_value != '' else f"sem {prop_name}"]})
                n+=1
            i+=1

        r = client.scroll(scroll='1m', scroll_id=r.get("_scroll_id"))
    client.clear_scroll(scroll_id=r.get("_scroll_id"))
    return n

def update_all(indice, prop_name, old_value, new_value, section):
    n = 0
    must = [{"term": {aggregation_map[prop_name][0]: old_value}}]
    if section:
        must.append({"term": {aggregation_map['Secção'][0]: section}})

    r = client.search(index=indice, source=[prop_name,"UUID"], scroll="2m", query={"bool": {"must": must}})
    i=0
    while i < r["hits"]["total"]["value"]:
        for hit in r["hits"]["hits"]:
            curr_value = hit["_source"][prop_name]
            if isinstance(curr_value, list):
                for idx, curr_old_value in enumerate(curr_value):
                    if curr_old_value == old_value:
                        curr_value[idx] = new_value
                client.update(index=indice, id=hit["_id"], doc={prop_name: curr_value})
                n+=1
            elif isinstance(curr_value, str):
                client.update(index=indice, id=hit["_id"], doc={prop_name: new_value})
                n+=1
            else:
                print(f"WARNING ON: {prop_name} {old_value} => {new_value} uuid {hit['_source']['UUID']})")
                print(f"            curr_value is {curr_value}")
                print(f"            update ignored")

            i+=1

        r = client.scroll(scroll='1m', scroll_id=r.get("_scroll_id"))
    
    client.clear_scroll(scroll_id=r.get("_scroll_id"))
    return n


if __name__ == "__main__":
    main()
