from modules.log import log
from modules.api_queries.api_queries_functions import ApiQueriesClient
from modules.minioclient.minio_client import MinioClient
import pandas as pd
import json
import numpy as np
from modules.retry import RetryOnException as retry


@log
class CrudeMortalityRateDag4:
    def __init__(self):
        self.logger.info("Inicio Logica")

    def main_logic(self, df):
        self.logger.info("Inicia main_logic")
        table_names = ["Anno", "Anno2", "Anno3"]
        groups = df[0].isin(table_names).cumsum()
        tables = {g.iloc[0,0]: g.iloc[1:] for k,g in df.groupby(groups)}

        for k,v in tables.items():
            if(k=="Anno"):
                df_year=v

        df_year.rename(columns = {0:'Anno', 1:'Causa', 2:'Subgrupo', 3:'SexoDesc', 4:'Valor'}, inplace = True)
        df_year=df_year[['Anno','Causa','Subgrupo','SexoDesc','Valor']].reset_index(drop=True)

        df_year_without_accents_a = df_year.replace(to_replace="[áÁ]", regex=True, value='a').replace(to_replace=",", regex=True, value='.')
        df_year_without_accents_e = df_year_without_accents_a.replace(to_replace="[éÉ]", regex=True, value='e')
        df_year_without_accents_i = df_year_without_accents_e.replace(to_replace="[íÍ]", regex=True, value='i')
        df_year_without_accents_o = df_year_without_accents_i.replace(to_replace="[óÓ]", regex=True, value='o')
        df_year_without_accents_u = df_year_without_accents_o.replace(to_replace="[úÚ]", regex=True, value='u')
        df_year = df_year_without_accents_u
        self.logger.info("Finaliza main_logic")
        return df_year

    @retry(3)
    def load_information(self, data, token, origin):
        self.logger.info("Inicio función load_information")
        api_queries = ApiQueriesClient()
        data = data.replace(to_replace=np.NaN, regex=True, value="null")
        count = 0
        for i in data.index:
            value = data.iloc[i]['Valor']
            if value is "null":
                value = None
            else:
                value = float(value)

            payload = json.dumps({
                "year": int(data.iloc[i]['Anno']),
                "cause": str(data.iloc[i]['Causa']),
                "subgroup": str(data.iloc[i]['Subgrupo']),
                "gender": str(data.iloc[i]['SexoDesc']),
                "value": value,
                "origin": origin
            })
            self.logger.info(payload)
            try:
                api_queries.post_api("/api/v1/raw-data/crude-mortality-rate/", payload, token)
                count = count + 1
            except Exception:
                self.logger.error("Error en comunicación API")

        self.logger.info("Total Registros:")
        self.logger.info(count)
        self.logger.info("Finaliza función load_information")

    def main_sispro(self, token):
        self.logger.info("Inicia Main")
        minio = MinioClient()
        name_bucket = "data-scraping-dags"
        x = minio.get_object(name_bucket, "staging/sispro_ts_bruta_mortalidad.xlsx", "sispro_ts_bruta_mortalidad.xlsx")
        df_country = pd.read_excel("sispro_ts_bruta_mortalidad.xlsx", sheet_name="PAIS", header=None, names=range(6), skiprows=1)
        self.logger.info("Carga de archivo exitoso PAIS")
        data_country = self.main_logic(df_country)
        self.load_information(data_country, token, "Colombia")
        df_department = pd.read_excel("sispro_ts_bruta_mortalidad.xlsx", sheet_name="DEPARTAMENTO", header=None, names=range(6), skiprows=1)
        self.logger.info("Carga de archivo exitoso DEPARTAMENTO")
        data_department = self.main_logic(df_department)
        self.load_information(data_department, token, "Meta")
        df_municipality = pd.read_excel("sispro_ts_bruta_mortalidad.xlsx", sheet_name="MUNICIPIO", header=None, names=range(6), skiprows=1)
        self.logger.info("Carga de archivo exitoso MUNICIPIO")
        data_municipality = self.main_logic(df_municipality)
        self.load_information(data_municipality, token, "Villavicencio")
        self.logger.info("Termina Main")
