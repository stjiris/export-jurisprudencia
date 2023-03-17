import lxml.html

name_to_field_and_key = {
    'Número de Processo': ('Número de Processo','key'),
    'ECLI': ('ECLI','key'),
    'Data' : ('Data', 'key_as_string'),
    'Relator Nome Profissional': ('Relator Nome Profissional.raw', 'key'),
    'Relator Nome Completo': ('Relator Nome Completo.raw', 'key'),
    'Descritores': ('Descritores.raw', 'key'),
    'Meio Processual': ('Meio Processual.raw', 'key'),
    'Votação - Decisão': ('Votação - Decisão.raw', 'key'),
    'Votação - Vencidos': ('Votação - Vencidos.raw', 'key'),
    'Votação - Declarações': ('Votação - Declarações.raw', 'key'),
    'Secção': ('Secção.raw', 'key'),
    'Área': ('Área.raw', 'key'),
    'Decisão - Decomposta': ('Decisão - Decomposta.raw', 'key'),
    'Decisão - Integral': ('Decisão - Integral.raw', 'key'),
    'Tribunal de Recurso - Tribunal': ('Tribunal de Recurso - Tribunal.raw','key'),
    'Tribunal de Recurso - Processo': ('Tribunal de Recurso - Processo.raw','key'),
    'Fonte': ('Fonte','key'),
    'Url': ('URL','key')
}

text_content = lambda html: lxml.html.fromstring(html).text_content().strip()

name_to_original_getter = {
    'Número de Processo': lambda o: text_content(o["Processo"]) if "Processo" in o else "",
    'ECLI': lambda o: o.get("ECLI") or o.get("ecli") or "",
    'Data' : lambda o: o.get("Data") or o.get("Data do Acordão") or o.get("Data da Decisão Sumária") or o.get("Data da Reclamação") or o.get("Data de decisão sumária"),
    'Relator Nome Profissional': lambda o: text_content(o["Relator"]) if "Relator" in o else "" or o.get("relator"),
    'Relator Nome Completo': lambda o: text_content(o["Relator"]) if "Relator" in o else "" or o.get("relator"),
    'Descritores': lambda o: text_content(o["Descritores"]) if "Descritores" in o else "" or o.get("descritores") or "",
    'Meio Processual': lambda o: text_content(o["Meio Processual"]) if "Meio Processual" in o else "",
    'Votação - Decisão': lambda o: text_content(o["Votação"]) if "Votação" in o else "",
    'Votação - Vencidos': lambda o: text_content(o["Votação"]) if "Votação" in o else "",
    'Votação - Declarações': lambda o: text_content(o["Votação"]) if "Votação" in o else "",
    'Secção': lambda o: text_content(o["Nº Convencional"]) if "Nº Convencional" in o else text_content(o["Área Temática"]) if "Área Temática" in o else "" or o.get("tematica") or "",
    'Área': lambda o:  text_content(o["Nº Convencional"]) if "Nº Convencional" in o else text_content(o["Área Temática"]) if "Área Temática" in o else "" or o.get("tematica") or "",
    'Decisão - Decomposta': lambda o: text_content(o["Decisão"]) if "Decisão" in o else "" or o.get("decisão") or "",
    'Decisão - Integral': lambda o: text_content(o["Decisão"]) if "Decisão" in o else "" or o.get("decisão") or "",
    'Tribunal de Recurso - Tribunal': lambda o: text_content(o["Tribunal Recurso"]) if "Tribunal Recurso" in o else "",
    'Tribunal de Recurso - Processo': lambda o: text_content(o["Processo no Tribunal Recurso"]) if "Processo no Tribunal Recurso" in o else "",
    'Fonte': lambda o: "",
    'Url': lambda o: ""
}