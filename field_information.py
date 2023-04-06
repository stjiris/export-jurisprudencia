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
    'Decisão': ('Decisão.raw', 'key'),
    'Decisão (textual)': ('Decisão (textual).raw', 'key'),
    'Tribunal de Recurso': ('Tribunal de Recurso.raw','key'),
    'Tribunal de Recurso - Processo': ('Tribunal de Recurso - Processo.raw','key'),
    'Área Temática': ('Área Temática.raw','key'),
    'Jurisprudência Estrangeira': ('Jurisprudência Estrangeira.raw','key'),
    'Jurisprudência Internacional': ('Jurisprudência Internacional.raw','key'),
    'Doutrina': ('Doutrina.raw','key'),
    'Jurisprudência Nacional': ('Jurisprudência Nacional.raw','key'),
    'Legislação Comunitária': ('Legislação Comunitária.raw','key'),
    'Legislação Estrangeira': ('Legislação Estrangeira.raw','key'),
    'Legislação Nacional': ('Legislação Nacional.raw','key'),
    'Referências Internacionais': ('Referências Internacionais.raw','key'),
    'Indicações Eventuais': ('Indicações Eventuais.raw','key'),
    'Referência de publicação': ('Referência de publicação.raw','key'),
    'Fonte': ('Fonte','key'),
    'URL': ('URL','key')
}

text_content = lambda html: lxml.html.fromstring(html).text_content().strip()


def referencias(o):
    r = ""
    r+= text_content(o["Jurisprudência Estrangeira"]) if "Jurisprudência Estrangeira" in o else ""
    r+= text_content(o["Jurisprudência Internacional"]) if "Jurisprudência Internacional" in o else ""
    r+= text_content(o["Doutrina"])if "Doutrina" in o else ""
    r+= text_content(o["Jurisprudência Nacional"]) if "Jurisprudência Nacional" in o else ""
    r+= text_content(o["Legislação Comunitária"]) if "Legislação Comunitária" in o else ""
    r+= text_content(o["Legislação Estrangeira"]) if "Legislação Estrangeira" in o else ""
    r+= text_content(o["Legislação Nacional"]) if "Legislação Nacional" in o else ""
    r+= text_content(o["Referências Internacionais"]) if "Referências Internacionais" in o else ""
    return r

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
    'Decisão': lambda o: text_content(o["Decisão"]) if "Decisão" in o else "" or o.get("decisão") or "",
    'Decisão (textual)': lambda o: text_content(o["Decisão"]) if "Decisão" in o else "" or o.get("decisão") or "",
    'Tribunal de Recurso': lambda o: text_content(o["Tribunal Recurso"]) if "Tribunal Recurso" in o else "",
    'Tribunal de Recurso - Processo': lambda o: text_content(o["Processo no Tribunal Recurso"]) if "Processo no Tribunal Recurso" in o else "",
    'Área Temática': lambda o: text_content(o["Área Temática"]) if "Área Temática" in o else "",
    'Referências': referencias,
    'Jurisprudência Estrangeira': lambda o: text_content(o["Jurisprudência Estrangeira"]) if "Jurisprudência Estrangeira" in o else "",
    'Jurisprudência Internacional': lambda o: text_content(o["Jurisprudência Internacional"]) if "Jurisprudência Internacional" in o else "",
    'Doutrina': lambda o: text_content(o["Doutrina"]) if "Doutrina" in o else "",
    'Jurisprudência Nacional': lambda o: text_content(o["Jurisprudência Nacional"]) if "Jurisprudência Nacional" in o else "",
    'Legislação Comunitária': lambda o: text_content(o["Legislação Comunitária"]) if "Legislação Comunitária" in o else "",
    'Legislação Estrangeira': lambda o: text_content(o["Legislação Estrangeira"]) if "Legislação Estrangeira" in o else "",
    'Legislação Nacional': lambda o: text_content(o["Legislação Nacional"]) if "Legislação Nacional" in o else "",
    'Referências Internacionais': lambda o: text_content(o["Referências Internacionais"]) if "Referências Internacionais" in o else "",
    'Indicações Eventuais': lambda o: text_content(o["Indicações Eventuais"]) if "Indicações Eventuais" in o else "",
    'Referência de publicação': lambda o: text_content(o["Referência de Publicação"]) if "Referência de Publicação" in o else "",
    'Fonte': lambda o: "",
    'URL': lambda o: ""
}
