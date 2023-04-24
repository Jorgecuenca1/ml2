from modules.api_queries.api_queries_functions import ApiQueriesClient
from modules.minioclient.minio_client import MinioClient
from modules.retry import RetryOnException as retry
from modules.log import log
import pandas as pd
import numpy as np
import json

@log
class OverallMortalityRateDag11:

    def __init__(self):
        self.logger.info("Inicio Logica")

    def main_logic(self, df):
        self.logger.info("Inicio función logica_principal")
        df_year_without_accents_a = df.replace(to_replace="[áÁ]", regex=True, value='a')
        df_year_without_accents_e = df_year_without_accents_a.replace(to_replace="[éÉ]", regex=True, value='e')
        df_year_without_accents_i = df_year_without_accents_e.replace(to_replace="[íÍ]", regex=True, value='i')
        df_year_without_accents_o = df_year_without_accents_i.replace(to_replace="[óÓ]", regex=True, value='o')
        df_year_without_accents_u = df_year_without_accents_o.replace(to_replace="[úÚ]", regex=True, value='u')
        df_year = df_year_without_accents_u.replace(to_replace="[Ññ]", regex=True, value='ni')

        for i in df_year.index:
            df_year.at[i, 'Año'] = df_year.at[i, 'Año'][7:]

        self.logger.info("Finaliza función logica_principal")
        return df_year

    @retry(3)
    def load_information(self, data, token, origin):
        self.logger.info("Inicio función carga_informacion")
        api_queries = ApiQueriesClient()
        data = data.replace(to_replace=np.NaN, regex=True, value="nulo")
        count = 0
        for i in data.index:
            try:
                death_numbers = data.iloc[i]['Número_de_Defunciones']
                total_two = data.iloc[i]['Textbox14']
                total_three = data.iloc[i]['Textbox12']
                total_four = data.iloc[i]['Textbox15']
                total_five = data.iloc[i]['Textbox16']
                if death_numbers == "nulo":
                    death_numbers = None
                else:
                    death_numbers = float(death_numbers)
                if total_two == "nulo":
                    total_two = None
                else:
                    total_two = float(total_two)
                if total_three == "nulo":
                    total_three = None
                else:
                    total_three = float(total_three)
                if total_four == "nulo":
                    total_four = None
                else:
                    total_four = float(total_four)
                if total_five == "nulo":
                    total_five = None
                else:
                    total_five = float(total_five)

                payload = json.dumps({
                    "gender": str.lower(data.iloc[i]['Sexo']),
                    "age_group": str.lower(data.iloc[i]['Grupo_Etáreo___ASIS']),
                    "year": int(data.iloc[i]['Año']),
                    "death_numbers": death_numbers,
                    "total_two": total_two,
                    "total_three": total_three,
                    "total_four": total_four,
                    "total_five": total_five,
                    "origin": origin
                })
                self.logger.info(payload)
                try:
                    api_queries.post_api("/api/v1/raw-data/overall-mortality-rate/", payload, token)
                    count = count + 1
                except Exception:
                    self.logger.error("Error en comunicación API")
            except Exception:
                self.logger.error("Error en casteo de string a float")
        self.logger.info("Total Registros:")
        self.logger.info(count)
        self.logger.info("Finaliza función carga_informacion")

    def main_sispro(self, token):
        self.logger.info("Inicio Main")
        minio = MinioClient()
        self.logger.info("Descargando Archivo")
        name_bucket ="data-scraping-dags"
        x = minio.get_object(name_bucket, "/staging/sispro_ts_morta_general.xlsx", "sispro_ts_morta_general.xlsx")
        df_country = pd.read_excel("sispro_ts_morta_general.xlsx", sheet_name="PAIS", decimal=",")
        self.logger.info("Carga de archivo exitoso PAIS")
        data_country = self.main_logic(df_country)
        self.load_information(data_country, token, "Colombia")
        df_department = pd.read_excel("sispro_ts_morta_general.xlsx", sheet_name="DEPARTAMENTO", decimal=",")
        self.logger.info("Carga de archivo exitoso DEPARTAMENTO")
        data_department = self.main_logic(df_department)
        self.load_information(data_department, token, "Meta")
        df_municipality = pd.read_excel("sispro_ts_morta_general.xlsx", sheet_name="MUNICIPIO", decimal=",")
        self.logger.info("Carga de archivo exitoso MUNICIPIO")
        data_municipality = self.main_logic(df_municipality)
        self.load_information(data_municipality, token, "Villavicencio")
        self.logger.info("Finaliza Main")
