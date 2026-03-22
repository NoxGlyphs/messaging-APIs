# APIs-mensajeria
Colección de 3 APIs para gestionar un sistema de mensajería. Desarrollado en python y usando los programas: neo4j, redis y mongo

### Dependencias
Se ha proporcionado un export del entorno de conda sobre el que se ha desarrollado la práctica "environment.yml". Para importarlo:
conda "nombre_env" create -f environment.yml


### Guía de ejecución
Todo el código se encuentra en "src" y el ejemplo de ejecución de la aplicación completa es "main.ipynb".

Para ejecutarlo lo único que puede ser necesario cambiar son las factory function de los clientes de cada SGBD, por si se tienen los servicios corriendo en puerto que no es el por defecto o se quiere usar otra base de datos.
Además, es posible que también haya que cambiar la contraseña de neo4j ya que este pide autentificación (es el segundo campo de la tupla en el notebook).

Después en "demo_subsistemas" se prueba la api de cada subsistema. Estas contienen el CRUD y utilidades para alguna query más que se pedía en la práctica. La lógica de negocio de la aplicación esta en "services.py".