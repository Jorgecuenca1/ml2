from modules.log import log
from modules.api_queries.api_queries_functions import ApiQueriesClient
from modules.minioclient.minio_client import MinioClient
import pandas as pd
import json
import numpy as np
from modules.retry import RetryOnException as retry


@log
class ManualIndicatorsDag1:
    def __init__(self):
        self.logger.info("Inicio Logica")

    @retry(3)
    def percentagep_regnancy_10_to_14_years(self, file_name, token):
        try:
            self.logger.info("Inicio función PercentagePregnancy10To14Years")
            api_queries = ApiQueriesClient()
            self.logger.info("conectado a API")
            data = pd.read_excel(file_name, sheet_name="INDICADOR_1")
            self.logger.info("Lectura correcta")
            count = 0
            for i in data.index:
                value = data.iloc[i]['%']
                if value is np.NaN:
                    value = None
                else:
                    value = float(value)
                payload = json.dumps({
                    "year": int(data.iloc[i]['AÑO']),
                    "number_cases": int(data.iloc[i]['# CASOS']),
                    "total": int(data.iloc[i]['TOTAL']),
                    "percentage": value,
                    "category": 1
                })
                self.logger.info(payload)
                try:
                    api_queries.post_api("/api/v1/processed-data/manual-data-human-indicator-result/", payload, token)
                    count = count + 1
                except Exception:
                    self.logger.error("Error en comunicación API")
            self.logger.info("Total Registros:")
            self.logger.info(count)
            self.logger.info("Finaliza función PercentagePregnancy10To14Years")
        except Exception:
            self.logger.info("No existe fuente para el % EMBARAZO DE 10 A 14")
        return

    @retry(3)
    def percentage_pregnancy_15_to_19_years(self, file_name, token):
        try:
            self.logger.info("Inicio función PercentagePregnancy15To19Years")
            api_queries = ApiQueriesClient()
            self.logger.info("conectado a API")
            data = pd.read_excel(file_name, sheet_name="INDICADOR_2")
            self.logger.info("Lectura correcta")
            count = 0
            for i in data.index:
                value = data.iloc[i]['%']
                if value is np.NaN:
                    value = None
                else:
                    value = float(value)
                payload = json.dumps({
                    "year": int(data.iloc[i]['AÑO']),
                    "number_cases": int(data.iloc[i]['# CASOS']),
                    "total": int(data.iloc[i]['TOTAL']),
                    "percentage": value,
                    "category": 2
                })
                self.logger.info(payload)
                try:
                    api_queries.post_api("/api/v1/processed-data/manual-data-human-indicator-result/", payload, token)
                    count = count + 1
                except Exception:
                    self.logger.error("Error en comunicación API")
            self.logger.info("Total Registros:")
            self.logger.info(count)
            self.logger.info("Finaliza función PercentagePregnancy15To19Years")
        except Exception:
            self.logger.info("No existe fuente para el % EMBARAZO DE 15 A 19")
        return

    @retry(3)
    def percentage_caesarean_delivery(self, file_name, token):
        try:
            self.logger.info("Inicio función PercentageCaesareanDelivery")
            api_queries = ApiQueriesClient()
            self.logger.info("conectado a API")
            data = pd.read_excel(file_name, sheet_name="INDICADOR_3")
            self.logger.info("Lectura correcta")
            count = 0
            for i in data.index:
                value = data.iloc[i]['%']
                if value is np.NaN:
                    value = None
                else:
                    value = float(value)
                payload = json.dumps({
                    "year": int(data.iloc[i]['AÑO']),
                    "number_cases": int(data.iloc[i]['# CASOS']),
                    "total": int(data.iloc[i]['TOTAL']),
                    "percentage": value,
                    "category": 3
                })
                self.logger.info(payload)
                try:
                    api_queries.post_api("/api/v1/processed-data/manual-data-human-indicator-result/", payload, token)
                    count = count + 1
                except Exception:
                    self.logger.error("Error en comunicación API")
            self.logger.info("Total Registros:")
            self.logger.info(count)
            self.logger.info("Finaliza función PercentageCaesareanDelivery")
        except Exception:
            self.logger.info("No existe fuente para % PARTOS POR CESAREA")
        return

    @retry(3)
    def percentage_coverage_with_4_or_more_npcs(self, file_name, token):
        try:
            self.logger.info("Inicio función PercentageCoverageWith4OrMoreNpcs")
            api_queries = ApiQueriesClient()
            self.logger.info("conectado a API")
            data = pd.read_excel(file_name, sheet_name="INDICADOR_4")
            self.logger.info("Lectura correcta")
            count = 0
            for i in data.index:
                value = data.iloc[i]['%']
                if value is np.NaN:
                    value = None
                else:
                    value = float(value)
                payload = json.dumps({
                    "year": int(data.iloc[i]['AÑO']),
                    "number_cases": int(data.iloc[i]['# CASOS']),
                    "total": int(data.iloc[i]['TOTAL']),
                    "percentage": value,
                    "category": 4
                })
                self.logger.info(payload)
                try:
                    api_queries.post_api("/api/v1/processed-data/manual-data-human-indicator-result/", payload, token)
                    count = count + 1
                except Exception:
                    self.logger.error("Error en comunicación API")
            self.logger.info("Total Registros:")
            self.logger.info(count)
            self.logger.info("Finaliza función PercentageCoverageWith4OrMoreNpcs")
        except Exception:
            self.logger.info("No existe fuente para COBERTURA CON 4 O MAS CPN")
        return

    @retry(3)
    def percentage_births_in_migrants(self, file_name, token):
        try:
            self.logger.info("Inicio función PercentageBirthsInMigrants")
            api_queries = ApiQueriesClient()
            self.logger.info("conectado a API")
            data = pd.read_excel(file_name, sheet_name="INDICADOR_5")
            self.logger.info("Lectura correcta")
            count = 0
            for i in data.index:
                value = data.iloc[i]['%']
                if value is np.NaN:
                    value = None
                else:
                    value = float(value)
                payload = json.dumps({
                    "year": int(data.iloc[i]['AÑO']),
                    "number_cases": int(data.iloc[i]['# CASOS']),
                    "total": int(data.iloc[i]['TOTAL']),
                    "percentage": value,
                    "category": 5
                })
                self.logger.info(payload)
                try:
                    api_queries.post_api("/api/v1/processed-data/manual-data-human-indicator-result/", payload, token)
                    count = count + 1
                except Exception:
                    self.logger.error("Error en comunicación API")
            self.logger.info("Total Registros:")
            self.logger.info(count)
            self.logger.info("Finaliza función PercentageBirthsInMigrants")
        except Exception:
            self.logger.info("No existe fuente para PARTOS EN MIGRANTES")
        return

    @retry(3)
    def number_of_women_killed_by_their_partner_or_ex_partner(self, file_name, token):
        try:
            self.logger.info("Inicio función NumberOfWomenKilledByTheirPartnerOrExPartner")
            api_queries = ApiQueriesClient()
            self.logger.info("conectado a API")
            data = pd.read_excel(file_name, sheet_name="INDICADOR_6")
            self.logger.info("Lectura correcta")
            count = 0
            for i in data.index:
                value = data.iloc[i]['# CASOS']
                if value is np.NaN:
                    value = None
                else:
                    value = int(value)
                payload = json.dumps({
                    "year": int(data.iloc[i]['AÑO']),
                    "number_cases": value,
                    "total": None,
                    "percentage": None,
                    "category": 6
                })
                self.logger.info(payload)
                try:
                    api_queries.post_api("/api/v1/processed-data/manual-data-human-indicator-result/", payload, token)
                    count = count + 1
                except Exception:
                    self.logger.error("Error en comunicación API")
            self.logger.info("Total Registros:")
            self.logger.info(count)
            self.logger.info("Finaliza función NumberOfWomenKilledByTheirPartnerOrExPartner")
        except Exception:
            self.logger.info("No existe fuente para # de mujeres asesinadas por su ")
        return

    @retry(3)
    def percentage_of_cases_of_partner_or_ex_partner_violence(self, file_name, token):
        try:
            self.logger.info("Inicio función PercentageOfCasesOfPartnerOrExPartnerViolence")
            api_queries = ApiQueriesClient()
            self.logger.info("conectado a API")
            data = pd.read_excel(file_name, sheet_name="INDICADOR_7")
            self.logger.info("Lectura correcta")
            count = 0
            for i in data.index:
                value = data.iloc[i]['%']
                if value is np.NaN:
                    value = None
                else:
                    value = float(value)
                payload = json.dumps({
                    "year": int(data.iloc[i]['AÑO']),
                    "number_cases": int(data.iloc[i]['# CASOS']),
                    "total": int(data.iloc[i]['TOTAL']),
                    "percentage": value,
                    "category": 7
                })
                self.logger.info(payload)
                try:
                    api_queries.post_api("/api/v1/processed-data/manual-data-human-indicator-result/", payload, token)
                    count = count + 1
                except Exception:
                    self.logger.error("Error en comunicación API")
            self.logger.info("Total Registros:")
            self.logger.info(count)
            self.logger.info("Finaliza función PercentageOfCasesOfPartnerOrExPartnerViolence")
        except Exception:
            self.logger.info("No existe fuente para % de casos de violencia de pare")
        return

    @retry(3)
    def percentage_of_cases_of_sexual_violence(self, file_name, token):
        try:
            self.logger.info("Inicio función PercentageOfCasesOfSexualViolence")
            api_queries = ApiQueriesClient()
            self.logger.info("conectado a API")
            data = pd.read_excel(file_name, sheet_name="INDICADOR_8")
            self.logger.info("Lectura correcta")
            count = 0
            for i in data.index:
                value = data.iloc[i]['%']
                if value is np.NaN:
                    value = None
                else:
                    value = float(value)
                payload = json.dumps({
                    "year": int(data.iloc[i]['AÑO']),
                    "number_cases": int(data.iloc[i]['# CASOS']),
                    "total": int(data.iloc[i]['TOTAL']),
                    "percentage": value,
                    "category": 8
                })
                self.logger.info(payload)
                try:
                    api_queries.post_api("/api/v1/processed-data/manual-data-human-indicator-result/", payload, token)
                    count = count + 1
                except Exception:
                    self.logger.error("Error en comunicación API")
            self.logger.info("Total Registros:")
            self.logger.info(count)
            self.logger.info("Finaliza función PercentageOfCasesOfSexualViolence")
        except Exception:
            self.logger.info("No existe fuente para % de casos de violencia sexual")
        return

    @retry(3)
    def rabies_vaccination_coverage(self, file_name, token):
        try:
            self.logger.info("Inicio función RabiesVaccinationCoverage")
            api_queries = ApiQueriesClient()
            self.logger.info("Conectado a API")
            data = pd.read_excel(file_name, sheet_name="INDICADOR_9")
            self.logger.info("Lectura correcta")
            count = 0
            for i in data.index:
                value = data.iloc[i]['% COBERTURA']
                valueTwo = data.iloc[i]['ANIMALES VACUNADOS']
                if value is np.NaN:
                    value = None
                else:
                    value = float(value)
                if valueTwo is np.NaN:
                    valueTwo = None
                else:
                    valueTwo = int(valueTwo)
                payload = json.dumps({
                    "year": int(data.iloc[i]['AÑO']),
                    "goal": int(data.iloc[i]['META']),
                    "vaccinated_animals": valueTwo,
                    "percentage_coverage": value,
                    "category": 1
                })
                self.logger.info(payload)
                try:
                    api_queries.post_api("/api/v1/processed-data/manual-data-animal-indicator-result/", payload, token)
                    count = count + 1
                except Exception:
                    self.logger.error("Error en comunicación API")
            self.logger.info("Total Registros:")
            self.logger.info(count)
            self.logger.info("Finaliza función RabiesVaccinationCoverage")
        except Exception:
            self.logger.info("No existe fuente para COBERTURA VACUNACIÓN ANTIRRABIC")
        return

    @retry(3)
    def main_manual_data(self, token):
        self.logger.info("Inicia Main")
        minio = MinioClient()
        name_bucket = "data-scraping-dags"
        x = minio.get_object(name_bucket, "staging/sources/INDICADORES_OBSERVATORIO_SP.xlsx", "INDICADORES_OBSERVATORIO_SP.xlsx")
        self.logger.info("Carga de archivo exitoso")
        file_name = "INDICADORES_OBSERVATORIO_SP.xlsx"
        self.percentagep_regnancy_10_to_14_years(file_name, token)
        self.percentage_pregnancy_15_to_19_years(file_name, token)
        self.percentage_caesarean_delivery(file_name, token)
        self.percentage_coverage_with_4_or_more_npcs(file_name, token)
        self.percentage_births_in_migrants(file_name, token)
        self.number_of_women_killed_by_their_partner_or_ex_partner(file_name, token)
        self.percentage_of_cases_of_partner_or_ex_partner_violence(file_name, token)
        self.percentage_of_cases_of_sexual_violence(file_name, token)
        self.rabies_vaccination_coverage(file_name, token)
        self.logger.info("Termina Main")