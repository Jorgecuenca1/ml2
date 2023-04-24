from modules.api_queries.api_queries_functions import ApiQueriesClient
from modules.minioclient.minio_client import MinioClient
from modules.retry import RetryOnException as retry
from modules.log import log
import pandas as pd
import json


@log
class DeathsDifferentCausesDag10:

    def __init__(self):
        self.logger.info("Inicio Logica")

    def main_logic(self, df):
        self.logger.info("Inicio función logica_principal")
        df.rename(columns={'Textbox27': 'Capitulo', 'Sexo': 'Sexo', 'Grupo_Etáreo___Quinquenios_DANE': 'Grupos_Edades',
                           'Año': 'Año', 'Textbox27': 'tasa_mortalidad', 'Textbox30': 'total_ts_dos',
                           'Textbox3': 'total_ts_tres', 'Textbox4': 'total_ts_cuatro', 'Textbox24': 'total_ts_cinco',
                           'Textbox31': 'total_ts_seis', 'Textbox15': 'total_ts_siete', 'Textbox33': 'total_ts_ocho'},
                  inplace=True)
        df_year_without_accents_a = df.replace(to_replace="[áÁ]", regex=True, value='a')
        df_year_without_accents_e = df_year_without_accents_a.replace(to_replace="[éÉ]", regex=True, value='e')
        df_year_without_accents_i = df_year_without_accents_e.replace(to_replace="[íÍ]", regex=True, value='i')
        df_year_without_accents_o = df_year_without_accents_i.replace(to_replace="[óÓ]", regex=True, value='o')
        df_year_without_accents_u = df_year_without_accents_o.replace(to_replace="[úÚ]", regex=True, value='u')
        df_year = df_year_without_accents_u.replace(to_replace="[Ññ]", regex=True, value='ni')
        df_year["tasa_mortalidad"] = [str(i).replace(".", "") for i in df_year["tasa_mortalidad"]]
        df_year["total_ts_dos"] = [str(i).replace(".", "") for i in df_year["total_ts_dos"]]
        df_year["total_ts_tres"] = [str(i).replace(".", "") for i in df_year["total_ts_tres"]]
        df_year["total_ts_cuatro"] = [str(i).replace(".", "") for i in df_year["total_ts_cuatro"]]
        df_year["total_ts_cinco"] = [str(i).replace(".", "") for i in df_year["total_ts_cinco"]]
        df_year["total_ts_seis"] = [str(i).replace(".", "") for i in df_year["total_ts_seis"]]
        df_year["total_ts_siete"] = [str(i).replace(".", "") for i in df_year["total_ts_siete"]]
        df_year["total_ts_ocho"] = [str(i).replace(".", "") for i in df_year["total_ts_ocho"]]

        for i in df_year.index:
            df_year.at[i, 'Año'] = df_year.at[i, 'Año'][7:]

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
                mortality_rate = data.iloc[i]['tasa_mortalidad']
                total_two = data.iloc[i]['total_ts_dos']
                total_three = data.iloc[i]['total_ts_tres']
                total_four = data.iloc[i]['total_ts_cuatro']
                total_five = data.iloc[i]['total_ts_cinco']
                total_six = data.iloc[i]['total_ts_seis']
                total_seven = data.iloc[i]['total_ts_siete']
                total_eight = data.iloc[i]['total_ts_ocho']

                if mortality_rate is "null":
                    mortality_rate = None
                else:
                    mortality_rate = float(mortality_rate)
                if total_two is "null":
                    total_two = None
                else:
                    total_two = float(total_two)
                if total_three is "null":
                    total_three = None
                else:
                    total_three = float(total_three)
                if total_four is "null":
                    total_four = None
                else:
                    total_four = float(total_four)

                if total_five is "null":
                    total_five = None
                else:
                    total_five = float(total_five)
                if total_six is "null":
                    total_six = None
                else:
                    total_six = float(total_six)
                if total_seven is "null":
                    total_seven = None
                else:
                    total_seven = float(total_seven)
                if total_eight is "null":
                    total_eight = None
                else:
                    total_eight = float(total_eight)

                payload = json.dumps({
                    "event": str.lower(data.iloc[i]['Capitulo']),
                    "gender": str.lower(data.iloc[i]['Sexo']),
                    "age_group": str.lower(data.iloc[i]['Grupos_Edades']),
                    "year": int(data.iloc[i]['Año']),
                    "mortality_rate": mortality_rate,
                    "total_two": total_two,
                    "total_three": total_three,
                    "total_four": total_four,
                    "total_five": total_five,
                    "total_six": total_six,
                    "total_seven": total_seven,
                    "total_eight": total_eight,
                    "origin": origin
                })
                self.logger.info(payload)
                try:
                    api_queries.post_api("/api/v1/raw-data/deaths-different-causes/", payload, token)
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
        x = minio.get_object(name_bucket, "/staging/sispro_defun_diferentes_causas.xlsx", "sispro_defun_diferentes_causas.xlsx")
        df_country = pd.read_excel("sispro_defun_diferentes_causas.xlsx", sheet_name="PAIS")
        self.logger.info("Carga de archivo exitoso PAIS")
        data_country = self.main_logic(df_country)
        self.load_information(data_country, token, "Colombia")
        df_department = pd.read_excel("sispro_defun_diferentes_causas.xlsx", sheet_name="DEPARTAMENTO")
        self.logger.info("Carga de archivo exitoso DEPARTAMENTO")
        data_department = self.main_logic(df_department)
        self.load_information(data_department, token, "Meta")
        df_municipality = pd.read_excel("sispro_defun_diferentes_causas.xlsx", sheet_name="MUNICIPIO")
        self.logger.info("Carga de archivo exitoso MUNICIPIO")
        data_municipality = self.main_logic(df_municipality)
        self.load_information(data_municipality, token, "Villavicencio")
        self.logger.info("Finaliza Main")
