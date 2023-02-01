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

def scroll_all(index, source, initial_func, foreach_func, final_func):
    r = client.search(index=index, source=source, scroll="2m")
    i = 0
    initial_func(r)
    while r["hits"]["total"]["value"] > 0:
        for hit in r["hits"]["hits"]:
            foreach_func(hit, i)
            i+=1
        print( f"Scrolling... {i}/{r['hits']['total']['value']}")
        r = client.scroll(scroll='1m', scroll_id=r.get("_scroll_id"))

    final_func()

aggregation_map = {
    'Data' : ('Data', 'key_as_string'),
    'Decisão': ('Decisão.raw', 'key'),
    'Descritores': ('Descritores.raw', 'key'),
    'Meio Processual': ('Meio Processual.raw', 'key'),
    'Processo': ('Processo', 'key'),
    'Relator': ('Relator.raw', 'key'),
    'Secção': ('Secção.raw', 'key'),
    'Votação': ('Votação.raw', 'key')
}
def aggregate_field(index, prop_name, output_folder):
    r = client.search(index=index, size=0, aggs={
        prop_name: {
            'terms': {
                'field': aggregation_map[prop_name][0],
                'size': 100000, #65536,
                'order': {
                    '_key': "asc",
                }
            }
        }
    })

    df = pd.DataFrame(columns=["Count","Valor Atual","Correção"], data=((h.get("doc_count"), h.get(aggregation_map[prop_name][1]), "") for h in r.get("aggregations").get(prop_name).get("buckets")))
    df.to_excel(path.join(output_folder, f"{prop_name}-aggs.xlsx"),index=False)

text_content = lambda html: lxml.html.fromstring(html).text_content().strip()
original_map = {
    'Data' : lambda o: o.get("Data") or o.get("Data do Acordão") or o.get("Data da Decisão Sumária") or o.get("Data da Reclamação"),
    'Decisão': lambda o: text_content(o["Decisão"]) if "Decisão" in o else "",
    'Descritores': lambda o: text_content(o["Descritores"]) if "Descritores" in o else "",
    'Meio Processual': lambda o: text_content(o["Meio Processual"]) if "Meio Processual" in o else "",
    'Processo': lambda o: text_content(o["Processo"]) if "Processo" in o else "",
    'Relator': lambda o: text_content(o["Relator"]) if "Relator" in o else "",
    'Secção': lambda o: text_content(o["Nº Convencional"]) if "Nº Convencional" in o else "",
    'Votação': lambda o: text_content(o["Votação"]) if "Votação" in o else "",
}

@click.command()
@click.argument("indice", required=True)
@click.option("-e","--exclude",multiple=True, help="Fields to ignore. Fields that are not text, keyword or date are already ignored.")
@click.option("-i","--index-column",required=False, help="Overwrites field to use as an ID. This value must be a key of _source")
@click.option("-o","--output-folder",required=False,type=click.Path(file_okay=False,dir_okay=True,exists=True,resolve_path=True), help="Overwrites output folder")
def main(indice,exclude,index_column, output_folder):
    """
        This tool will exports INDICE into .xlsx files under the INDICE folder for each field in the INDICE.
        Each .xlsx file has the following columns:

         | ID | Original | Atual | Correção |

         |----|----------|-------|----------|
    """
    if not output_folder:
        makedirs(indice, exist_ok=True)
        output_folder = indice
    properties = client.indices.get_mapping(index=indice)[indice]["mappings"]["properties"]
    source = ["Original"]
    saving_props = []

    if not index_column:
        get_index_name = lambda x: x["_id"]
    else:
        get_index_name = lambda x: x["_source"][index_column]
        source.append(index_column)

    get_original_value = lambda hit, prop: original_map[prop](hit["_source"]["Original"])

    for prop_name in properties:
        prop_info = properties[prop_name]
        prop_type = prop_info.get("type", None)
        if prop_type != 'text' and prop_type != 'keyword' and prop_type != 'date' or prop_name in exclude:
            continue
        saving_props.append(prop_name)
        source.append(prop_name)
    
    data_frames = {}
    def prepare_pandas(first_result):
        size = first_result["hits"]["total"]["value"]
        for prop_name in saving_props:
            data_frames[prop_name] = pd.DataFrame(index=np.arange(size), columns=["ID","Original","Atual","Correção"])
    def foreach_hit(hit, index):
        for prop_name in saving_props:
            if isinstance(hit["_source"][prop_name], str):
                data_frames[prop_name].iloc[index] = [get_index_name(hit),get_original_value(hit, prop_name),hit["_source"][prop_name],""]
            elif hit["_source"][prop_name]:
                data_frames[prop_name].iloc[index] = [get_index_name(hit),get_original_value(hit, prop_name),"\n".join(hit["_source"][prop_name]),""]
    def finalize_pandas():
        for prop_name in saving_props:
            data_frames[prop_name].to_excel(path.join(output_folder, f"{prop_name}.xlsx"),index=False)
    
    scroll_all(indice, list(source), prepare_pandas, foreach_hit, finalize_pandas)
    
    for prop_name in saving_props:
        aggregate_field(indice, prop_name, output_folder)


if __name__ == "__main__":
    main()


    