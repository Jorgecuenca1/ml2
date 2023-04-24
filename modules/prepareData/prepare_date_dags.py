import pandas as pd
import os
from modules.log import log


@log
class PrepareData:

  def __init__(self, path):
    self.path = path
    self.df_country = None
    self.df_deparment = None
    self.df_county = None
    self.logger.info(f"{os.listdir(path)}")

  def source4(self, country, department, county):
    self.df_country = pd.read_csv(f"{self.path}/{country}", header=None, names=range(6), skiprows=2)
    self.df_deparment = pd.read_csv(f"{self.path}/{department}", header=None, names=range(6), skiprows=2)
    self.df_county = pd.read_csv(f"{self.path}/{county}", header=None, names=range(6), skiprows=2)

  def source5and6(self, country, department, county):
    self.df_country = pd.read_csv(f"{self.path}/{country}", header=None, names=range(3), skiprows=1,
                                  decimal=',', encoding='utf-8')
    self.df_deparment = pd.read_csv(f"{self.path}/{department}", header=None, names=range(3), skiprows=1,
                                    decimal=',', encoding='utf-8')
    self.df_county = pd.read_csv(f"{self.path}/{county}", header=None, names=range(3), skiprows=1,
                                 decimal=',', encoding='utf-8')

  def source7(self, country, department, county):
    self.df_country = pd.read_csv(f"{self.path}/{country}", header=None, names=range(5), skiprows=2)
    self.df_deparment = pd.read_csv(f"{self.path}/{department}", header=None, names=range(5), skiprows=2)
    self.df_county = pd.read_csv(f"{self.path}/{county}", header=None, names=range(5), skiprows=2)

  def source8(self, country, department, county):
    self.df_country = pd.read_csv(f"{self.path}/{country}", header=None, names=range(5), skiprows=2, decimal=',')
    self.df_deparment = pd.read_csv(f"{self.path}/{department}", header=None, names=range(5), skiprows=2, decimal=',')
    self.df_county = pd.read_csv(f"{self.path}/{county}", header=None, names=range(5), skiprows=2, decimal=',')

  def source9(self, country, department, county):
    self.df_country = pd.read_csv(f"{self.path}/{country}", header=None, names=range(7), skiprows=2)
    self.df_deparment = pd.read_csv(f"{self.path}/{department}", header=None, names=range(7), skiprows=2)
    self.df_county = pd.read_csv(f"{self.path}/{county}", header=None, names=range(7), skiprows=2)

  def source10(self, country, department, county):
    pass

  def source11_14(self, country, department, county):
    self.df_country = pd.read_csv(f"{self.path}/{country}", skiprows=3, decimal=',').fillna(0)
    self.df_deparment = pd.read_csv(f"{self.path}/{department}", skiprows=3, decimal=',').fillna(0)
    self.df_county = pd.read_csv(f"{self.path}/{county}", skiprows=3, decimal=',').fillna(0)

  def source15(self, country, department, county):
    self.df_country = pd.read_csv(f"{self.path}/{country}", skiprows=3, decimal=',').fillna(0)
    self.df_deparment = pd.read_csv(f"{self.path}/{department}", skiprows=3, decimal=',').fillna(0)
    self.df_county = pd.read_csv(f"{self.path}/{county}", skiprows=3, decimal=',').fillna(0)

  def rename_file(self, file_name, new_name):
    path_old_name = f"{self.path}/{file_name}"
    path_new_name = f"{self.path}/{new_name}"
    self.logger.info(f"{path_old_name}")
    self.logger.info(f"{path_new_name}")
    if os.path.exists(path_new_name):
        os.remove(path_new_name)
        self.logger.info(f"Found and remove file {new_name}")
    if os.path.exists(path_old_name):
        os.rename(path_old_name, path_new_name)
        self.logger.info(f"Rename file {file_name} to {new_name}")
    else:
        self.logger.info("Wont any action")
    return path_new_name

  def saveFiles(self, name):
    with pd.ExcelWriter(f"{self.path}/{name}") as writer:
      self.df_country.to_excel(writer, sheet_name="PAIS", index=False)
      self.df_deparment.to_excel(writer, sheet_name="DEPARTAMENTO", index=False)
      self.df_county.to_excel(writer, sheet_name="MUNICIPIO", index=False)

  def dag10_11(self, name):
    self.df_country['Lugar'] = 'PAIS'
    self.df_deparment['Lugar'] = 'DEPARTAMENTO'
    self.df_county['Lugar'] = 'MUNICIPIO'
    self.df_final = pd.concat([self.df_country, self.df_deparment, self.df_county])
    self.df_final.to_csv(f"{self.path}/{name}", index=False, decimal='.')

  def cast_int(self, columns):
    for col in columns:
      self.df_country[col] = self.df_country[col].replace('\D+', '', regex=True).fillna(0).astype(int)
      self.df_deparment[col] = self.df_deparment[col].replace('\D+', '', regex=True).fillna(0).astype(int)
      self.df_county[col] = self.df_county[col].replace('\D+', '', regex=True).fillna(0).astype(int)

