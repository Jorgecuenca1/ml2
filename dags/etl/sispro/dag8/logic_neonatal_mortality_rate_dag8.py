from modules.api_queries.api_queries_functions import ApiQueriesClient
from modules.minioclient.minio_client import MinioClient
from modules.retry import RetryOnException as retry
from modules.log import log
import pandas as pd
import json

@log
class NeonatalMortalityRateDag8:

    def __init__(self):
        self.logger.info("Inicio Logica")

    def main_logic(self, df):
        self.logger.info("Inicio función logica_principal")
        df.rename(columns={0: 'Indicador', 1: 'Sexo', 2: 'Año', 3: 'ValorIndicador', 4: 'Numerador3', 5: 'Numerador1',
                           6: 'Numerador4'}, inplace=True)
        df = df.iloc[1:].reset_index(drop=True)
        df_year_without_accents_a = df.replace(to_replace="[áÁ]", regex=True, value='a')
        df_year_without_accents_e = df_year_without_accents_a.replace(to_replace="[éÉ]", regex=True, value='e')
        df_year_without_accents_i = df_year_without_accents_e.replace(to_replace="[íÍ]", regex=True, value='i')
        df_year_without_accents_o = df_year_without_accents_i.replace(to_replace="[óÓ]", regex=True, value='o')
        df_year_without_accents_u = df_year_without_accents_o.replace(to_replace="[úÚ]", regex=True, value='u')
        df_year = df_year_without_accents_u.replace(to_replace="[Ññ]", regex=True, value='ni')
        df_year["ValorIndicador"] = [str(i).replace(",", ".") for i in df_year["ValorIndicador"]]
        df_year["Numerador3"] = [str(i).replace(",", ".") for i in df_year["Numerador3"]]
        df_year["Numerador1"] = [str(i).replace(",", ".") for i in df_year["Numerador1"]]
        df_year["Numerador4"] = [str(i).replace(",", ".") for i in df_year["Numerador4"]]

        for i in df_year.index:
            df_year.at[i, 'Año'] = df_year.at[i, 'Año'][8:]

        self.logger.info("Finaliza función logica_principal")
        return df_year

    @retry(3)
    def load_information(self, data, token, origin):
        self.logger.info("Inicio función carga_informacion")
        api_queries = ApiQueriesClient()
        data = data.replace(to_replace="nan", regex=True, value="null")
        count = 0
        for i in data.index:
            try:
                indicator_value = data.iloc[i]['ValorIndicador']
                total_sex = data.iloc[i]['Numerador3']
                total_year = data.iloc[i]['Numerador1']
                total = data.iloc[i]['Numerador4']
                if indicator_value is "null":
                    indicator_value = None
                else:
                    indicator_value = float(indicator_value)
                if total_sex is "null":
                    total_sex = None
                else:
                    total_sex = float(total_sex)
                if total_year is "null":
                    total_year = None
                else:
                    total_year = float(total_year)
                if total is "null":
                    total = None
                else:
                    total = float(total)

                payload = json.dumps({
                    "indicator": str.lower(data.iloc[i]['Indicador']),
                    "gender": str.lower(data.iloc[i]['Sexo']),
                    "year": int(data.iloc[i]['Año']),
                    "indicator_value": indicator_value,
                    "total_sex": total_sex,
                    "total_year": total_year,
                    "total": total,
                    "origin": origin
                })

                self.logger.info(payload)
                try:
                    api_queries.post_api("/api/v1/raw-data/neonatal-mortality-rate/", payload, token)
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
        name_bucket = "data-scraping-dags"
        x = minio.get_object(name_bucket, "/staging/sispro_ts_morta_neonatal.xlsx", "sispro_ts_morta_neonatal.xlsx")
        df_country = pd.read_excel("sispro_ts_morta_neonatal.xlsx", sheet_name="PAIS", header=None, names=range(7), skiprows=1)
        self.logger.info("Carga de archivo exitoso PAIS")
        data_country = self.main_logic(df_country)
        self.load_information(data_country, token, "Colombia")
        df_department = pd.read_excel("sispro_ts_morta_neonatal.xlsx", sheet_name="DEPARTAMENTO", header=None, names=range(7), skiprows=1)
        self.logger.info("Carga de archivo exitoso DEPARTAMENTO")
        data_department = self.main_logic(df_department)
        self.load_information(data_department, token, "Meta")
        df_municipality = pd.read_excel("sispro_ts_morta_neonatal.xlsx", sheet_name="MUNICIPIO", header=None, names=range(7), skiprows=1)
        self.logger.info("Carga de archivo exitoso MUNICIPIO")
        data_municipality = self.main_logic(df_municipality)
        self.load_information(data_municipality, token, "Villavicencio")
        self.logger.info("Finaliza Main")
