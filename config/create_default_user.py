import os
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser

user = PasswordUser(models.User())
user.username = os.getenv('AIRFLOW_WEBSERVER_USER', 'admin')
user.email = os.getenv('AIRFLOW_WEBSERVER_EMAIL', 'admin@admin.com')
user.password = os.getenv('AIRFLOW_WEBSERVER_PASS', 'admin')
session = settings.Session()
session.add(user)
session.commit()
session.close()
exit()
