import click
from elasticsearch import Elasticsearch
from dotenv import load_dotenv, find_dotenv 
from os import makedirs, environ, path
import lxml.html

import pandas as pd
import numpy as np

# READ .env file if any
load_dotenv(find_dotenv())
ELASTICSEARCH_URL = environ.get("ELASTICSEARCH_URL", "http://localhost:9200/")
ELASTICSEARCH_USER = environ.get("ELASTICSEARCH_USER","")
ELASTICSEARCH_PASS = environ.get("ELASTICSEARCH_PASS","")

client = Elasticsearch(ELASTICSEARCH_URL, basic_auth=(ELASTICSEARCH_USER,ELASTICSEARCH_PASS))

aggregation_map = {
    'Data' : ('Data', 'key_as_string'),
    'Decisão': ('Decisão.raw', 'key'),
    'Descritores': ('Descritores.raw', 'key'),
    'Meio Processual': ('Meio Processual.raw', 'key'),
    'Relator': ('Relator.raw', 'key'),
    'Secção': ('Secção.raw', 'key'),
    'Votação': ('Votação.raw', 'key')
}

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
    props = ['Data','Decisão','Descritores','Meio Processual','Relator','Secção','Votação']
    xls = pd.ExcelFile(file)
    if not all(p in xls.sheet_names for p in props):
        print("Expected excel to have all the following sheets:", ", ".join(props))
        exit(1)

    for prop_name in props:
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
    must = [{"term": {"UUID": uuid}}]
    client.search(index=indice, source=[prop_name], scroll="2m", query={"bool": {"must": must}})
    while i < r["hits"]["total"]["value"]:
        for hit in r["hits"]["hits"]:
            curr_value = hit["_source"][prop_name]
            if isinstance(curr_value, list):
                if old_value == "":
                    curr_value.append(new_value)
                for idx, curr_old_value in enumerate(curr_value):
                    if curr_old_value == old_value:
                        curr_value[idx] = new_value
                client.update(index=indice, id=hit["_id"], doc={prop_name: [v for v in curr_value if v != '']})
                n+=1
            elif isinstance(curr_value, str):
                client.update(index=indice, id=hit["_id"], doc={prop_name: new_value if new_value != '' else f"sem {prop_name}"})
                n+=1
            else:
                raise RuntimeError("Unexpected value, it is not a string nor a list.")
            i+=1

        r = client.scroll(scroll='1m', scroll_id=r.get("_scroll_id"))
    return n

def update_all(indice, prop_name, old_value, new_value, section):
    n = 0
    must = [{"term": {aggregation_map[prop_name][0]: old_value}}]
    if section:
        must.append({"term": {aggregation_map['Secção'][0]: section}})

    r = client.search(index=indice, source=[prop_name], scroll="2m", query={"bool": {"must": must}})
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
                raise RuntimeError("Unexpected value, it is not a string nor a list.")
            i+=1

        r = client.scroll(scroll='1m', scroll_id=r.get("_scroll_id"))
    return n


if __name__ == "__main__":
    main()
