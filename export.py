import click
from elasticsearch import Elasticsearch
from dotenv import load_dotenv, find_dotenv 
from os import makedirs, environ, path

import pandas as pd
import numpy as np

# READ .env file if any
load_dotenv(find_dotenv())
ELASTICSEARCH_URL = environ.get("ELASTICSEARCH_URL", "http://localhost:9200/")
ELASTICSEARCH_USER = environ.get("ELASTICSEARCH_USER","")
ELASTICSEARCH_PASS = environ.get("ELASTICSEARCH_PASS","")

client = Elasticsearch(ELASTICSEARCH_URL, basic_auth=(ELASTICSEARCH_USER,ELASTICSEARCH_PASS))
indices_response = client.indices.get_alias()

def scroll_all(index, source, initial_func, foreach_func, final_func):
    r = client.search(index=index, source=source, scroll="2m")
    i = 0
    initial_func(r)
    while i < r["hits"]["total"]["value"]:
        for hit in r["hits"]["hits"]:
            foreach_func(hit, i)
            i+=1
        print( f"Scrolling... {i}/{r['hits']['total']['value']}")
        r = client.scroll(scroll='1m', scroll_id=r.get("_scroll_id"))

    final_func()

@click.command()
@click.argument("indice", required=True, type=click.Choice(indices_response.keys(), case_sensitive=True))
@click.option("-e","--exclude",multiple=True)
@click.option("-i","--index-column",required=False)
@click.option("-o","--output-folder",required=False,type=click.Path(file_okay=False,dir_okay=True,exists=True,resolve_path=True))
def main(indice,exclude,index_column, output_folder):
    if not output_folder:
        makedirs(indice, exist_ok=True)
        output_folder = indice
    properties = client.indices.get_mapping(index=indice)[indice]["mappings"]["properties"]
    source = []
    saving_props = []

    if not index_column:
        get_index_name = lambda x: x["_id"]
    else:
        get_index_name = lambda x: x["_source"][index_column]
        source.append(index_column)

    for prop_name in properties:
        prop_info = properties[prop_name]
        prop_type = prop_info.get("type", None)
        prop_fielddata = prop_info.get("fielddata", False)
        if prop_type != 'text' and prop_type != 'keyword' and prop_type != 'date' or prop_name in exclude:
            continue
        saving_props.append(prop_name)
        source.append(prop_name)

    data_frames = {}
    def prepare_pandas(first_result):
        size = first_result["hits"]["total"]["value"]
        for prop_name in saving_props:
            data_frames[prop_name] = pd.DataFrame(index=np.arange(size), columns=["ID","Atual","Correção"])
    def foreach_hit(hit, index):
        for prop_name in saving_props:
            if isinstance(hit["_source"][prop_name], str):
                data_frames[prop_name].iloc[index] = [get_index_name(hit),hit["_source"][prop_name],""]
            elif hit["_source"][prop_name]:
                data_frames[prop_name].iloc[index] = [get_index_name(hit),"\n".join(hit["_source"][prop_name]),""]
    def finalize_pandas():
        for prop_name in saving_props:
            data_frames[prop_name].to_excel(path.join(output_folder, f"{prop_name}.xlsx"),index=False)
    scroll_all(indice, list(source), prepare_pandas, foreach_hit, finalize_pandas)


if __name__ == "__main__":
    main()


    