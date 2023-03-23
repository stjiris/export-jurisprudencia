import click
from elasticsearch import Elasticsearch
from dotenv import load_dotenv, find_dotenv 
from os import makedirs, environ, path
import re
from field_information import name_to_field_and_key as aggregation_map, name_to_original_getter as original_map

import pandas as pd
import numpy as np

# READ .env file if any
load_dotenv(find_dotenv())
ELASTICSEARCH_URL = environ.get("ELASTICSEARCH_URL", "http://localhost:9200/")
ELASTICSEARCH_USER = environ.get("ELASTICSEARCH_USER","")
ELASTICSEARCH_PASS = environ.get("ELASTICSEARCH_PASS","")

client = Elasticsearch(ELASTICSEARCH_URL, basic_auth=(ELASTICSEARCH_USER,ELASTICSEARCH_PASS))

def scroll_all(index, source, initial_func, foreach_func, final_func):
    r = client.search(index=index, source=source, scroll="2m",size=200)
    i = 0
    initial_func(r)
    while i < r["hits"]["total"]["value"]:
        for hit in r["hits"]["hits"]:
            foreach_func(hit, i)
            i+=1
        print( f"Scrolling... {i}/{r['hits']['total']['value']}")
        r = client.scroll(scroll='1m', scroll_id=r.get("_scroll_id"))

    final_func()

def aggregate_field(index, prop_name, excel_writer):
    field_cardinality = client.search(index=index, size=0, aggs={
        prop_name: {
            'cardinality': {
                'field': aggregation_map[prop_name][0]
            }
        }
    }).get("aggregations").get(prop_name).get("value")
    num_parts = int(np.ceil(field_cardinality / 1000))
    c=0
    print("Aggregate", prop_name, "in", num_parts,"partitions")
    for i in range(num_parts+1):
        r = client.search(index=index, size=0, aggs={
            prop_name: {
                'terms': {
                    'field': aggregation_map[prop_name][0],
                    'include': {
                        'partition': i,
                        'num_partitions': num_parts
                    },
                    'size': 100000, #65536,
                    'order': {
                        '_key': "asc",
                    },
                    'missing': f'sem {prop_name}' if prop_name != 'Data' else '01/01/0001'
                },
                'aggs': {
                    'Secções': {
                        'terms': {
                            'field': aggregation_map["Secção"][0],
                            'size': 15       
                        }
                    }                
                }
            }
        })
        data = []
        for agg in r.get("aggregations").get(prop_name).get("buckets"):
            # "<empty>","curr","*","<count>"
            # data.append(("", agg.get(aggregation_map[prop_name][1]), "*", agg.get("doc_count"))) # DONT ADD *
            c+=agg.get("doc_count")
            # "<empty>","curr","Secção 1","<count sec 1>"
            data.extend(("", agg.get(aggregation_map[prop_name][1]), h.get("key"), h.get("doc_count")) for h in agg.get("Secções").get("buckets"))
            
        df = pd.DataFrame(columns=["Correção","Atual","Secção","Count"], data=data)
        df.to_excel(excel_writer, prop_name, index=False)
    return c

@click.command()
@click.argument("indice", required=True)
@click.option("-e","--export",multiple=True, help="Fields to export.")
@click.option("-i","--index-column",required=False, help="Overwrites field to use as an ID. This value must be a key of _source")
@click.option("-o","--output-folder",required=False,type=click.Path(file_okay=False,dir_okay=True,exists=True,resolve_path=True), help="Overwrites output folder")
@click.option("-n","--name", required=True, help="Filename suffix")
@click.option("-a","--all","create_indices", help="Create indices-<name>.xlsx", is_flag=True)
def main(indice,export,index_column, output_folder, name, create_indices):
    """
        This tool will exports INDICE into .xlsx files under the INDICE folder for each field in the INDICE.
        Each .xlsx file has the following columns:

         | Correção | ID | Original | Atual | Secção |
         |----------|----|----------|-------|--------|
    """
    if not output_folder:
        makedirs(indice, exist_ok=True)
        output_folder = indice
    properties = client.indices.get_mapping(index=indice)[indice]["mappings"]["properties"]
    source = ["Original", "Secção"]
    saving_props = []

    if not index_column:
        get_index_name = lambda x: x["_id"]
    else:
        get_index_name = lambda x: x["_source"][index_column]
        source.append(index_column)

    get_original_value = lambda hit, prop: original_map[prop](hit["_source"]["Original"])

    for prop_name in properties:
        if prop_name not in aggregation_map or (len(export) > 0  and prop_name not in export):
            continue
        saving_props.append(prop_name)
        source.append(prop_name)
    
    prop_count = {}
    data_frames = {}
    with pd.ExcelWriter(f'{output_folder}/aggs-{name}.xlsx') as writer:
        for prop_name in saving_props:        
            sizeAgg = aggregate_field(indice, prop_name, writer)
            data_frames[prop_name] = pd.DataFrame(index=np.arange(sizeAgg), columns=["Correção","ID","Original","Atual","Secção"])
            prop_count[prop_name] = 0

    def prepare_pandas(first_result):
        pass
    def foreach_hit(hit, index):
        for prop_name in saving_props:
            if isinstance(hit["_source"][prop_name], str):
                data_frames[prop_name].iloc[prop_count[prop_name]] = ["",get_index_name(hit),get_original_value(hit, prop_name),hit["_source"][prop_name],hit["_source"]["Secção"]]
                prop_count[prop_name]+=1
            elif hit["_source"][prop_name]:
                originalValues = list(set(re.split("\n|;",get_original_value(hit, prop_name)))) # Remove duplicates (they don't appear in aggs)
                for i, value in enumerate(set(hit["_source"][prop_name])):
                    data_frames[prop_name].iloc[prop_count[prop_name]] = ["",get_index_name(hit),originalValues[i] if i < len(originalValues) else "",value,hit["_source"]["Secção"]]
                    prop_count[prop_name]+=1
            else:
                data_frames[prop_name].iloc[prop_count[prop_name]] = ["",get_index_name(hit),get_original_value(hit, prop_name),f"sem {prop_name}",hit["_source"]["Secção"]]
                prop_count[prop_name]+=1


    def finalize_pandas():
        with pd.ExcelWriter(f'{output_folder}/indices-{name}.xlsx') as writer:
            for prop_name in saving_props:
                data_frames[prop_name].to_excel(writer, prop_name, index=False)
    if create_indices:
        scroll_all(indice, list(source), prepare_pandas, foreach_hit, finalize_pandas)


if __name__ == "__main__":
    main()


    