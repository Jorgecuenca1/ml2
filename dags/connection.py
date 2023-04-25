import psycopg2

# Parámetros de conexión
host = "130.211.118.80"
database = "postgres"
user = "postgres"
password = "ml2password"

date_value = "2023-04-24"
location_value = "España"
date2_value = "2023-04-25"
prediccion_value = "Lloverá"

class Pg:

    def connect(self):
        try:
            conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )

        # Si la conexión fue exitosa, creamos un cursor para ejecutar consultas
            cursor = conn.cursor()
         # Ejecutamos la consulta para insertar una nueva fila en la tabla
        # Ejecutamos la consulta para insertar una nueva fila en la tabla
            cursor.execute("INSERT INTO clima (Date, Location, date2, prediccion) VALUES (%s, %s, %s, %s)", (date_value, location_value, date2_value, prediccion_value))
            cursor.execute("INSERT INTO clima (Date, Location, date2, prediccion) VALUES (%s, %s, %s, %s)", ("2023-04-24", "rusia", "2023-04-25", "llovera"))
            cursor.execute("INSERT INTO clima (Date, Location, date2, prediccion) VALUES (%s, %s, %s, %s)", ("2023-04-24", "rusia", "2023-04-26", "llovera"))
            cursor.execute("INSERT INTO clima (Date, Location, date2, prediccion) VALUES (%s, %s, %s, %s)", ("2023-04-24", "rusia", "2023-04-27", "llovera"))
            cursor.execute("INSERT INTO clima (Date, Location, date2, prediccion) VALUES (%s, %s, %s, %s)", ("2023-04-24", "rusia", "2023-04-28", "llovera"))
            cursor.execute("INSERT INTO clima (Date, Location, date2, prediccion) VALUES (%s, %s, %s, %s)", ("2023-04-24", "rusia", "2023-04-29", "no llovera"))
            cursor.execute("INSERT INTO clima (Date, Location, date2, prediccion) VALUES (%s, %s, %s, %s)", ("2023-04-24", "rusia", "2023-04-30", "llovera"))
            cursor.execute("INSERT INTO clima (Date, Location, date2, prediccion) VALUES (%s, %s, %s, %s)", ("2023-04-24", "rusia", "2023-05-01", "no llovera"))
        # Hacemos commit para guardar los cambios en la base de datos
            conn.commit()
        # Ejecutamos una consulta de ejemplo
            cursor.execute("SELECT * FROM clima")
            print("ingreso")
        # Mostramos los resultados de la consulta
            rows = cursor.fetchall()
            for row in rows:
                print(row)

    # Cerramos el cursor y la conexión
            cursor.close()
            conn.close()

# Si la conexión falla, mostramos un mensaje de error
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
