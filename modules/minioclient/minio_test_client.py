from modules.minioclient.minio_client import MinioClient

bucket_name_test = "test"

minio = MinioClient()

# para crear un bucket se le envia el nombre del bucket
print(minio.create_bucket(bucket_name_test))

# para comprobar si un bucket existe se le envia el nombre del bucket
print(minio.bucket_exists(bucket_name_test))

# para obtener todos los buckets que existan
print(minio.list_buckets())

# para subir un archivo se le envia el nombre del bucket,
# el nombre del archivo de como se guardara en el bucket y la ruta de donde se encuentra el mismo
print(minio.put_object(bucket_name_test, "test.py",
                       "C:\\Users\\diego\\Desarrollos\\OSPV\\osv-scheduler\\modules\\minioclient\\minio_client.py"))

# para obtener un objeto se le envia el nombre del bucket,
# el nombre del archivo de como se guardo en el bucket y la ruta y nombre del archivo de donde lo quiere guardar en local
x = minio.get_object(bucket_name_test, "test.py", "test_download.py")
print(x.bucket_name, x.object_name)

# para listar los objetos existentes de un bucket se le pasa el nombre del bucket
for x in minio.list_objects(bucket_name_test):
    print(x.bucket_name, x.object_name)

# para remover un objeto de un bucket se le manda el nombre del bucket y el nombre del archivo/objeto
minio.remove_object(bucket_name_test, "test.py")

# para remover un bucket se le manda el nombre del bucket
minio.remove_bucket(bucket_name_test)
