# Tópicos en Telemática

# Estudiantes: 
- Lorena Goez Ruiz, lgoezr1@eafit.edu.co 
- Samuel Valencia Loaiza, svalenci41@eafit.edu.co

# Profesor: 
Edwin Nelson Valencia  

---

# Proyecto: GridFS – Sistema de Archivos Distribuido

## 1. Breve descripción de la actividad

El proyecto consiste en la implementación de un sistema de archivos distribuido similar a HDFS, denominado GridFS.  
El sistema permite almacenar archivos grandes dividiéndolos en bloques de tamaño fijo (4MB) y distribuyéndolos entre múltiples nodos de datos.  
La coordinación de usuarios, autenticación y metadatos es manejada por un nodo central (NameNode).  
Un cliente en línea de comandos (CLI) facilita la interacción con el sistema (registro, login, carga, descarga y listado de archivos).

### 1.1. Aspectos cumplidos o desarrollados de la actividad

- Autenticación de usuarios con **registro y login**.  
- Manejo de **tokens de sesión**.  
- NameNode que coordina la arquitectura distribuida.  
- 4 DataNodes configurados en **Docker Compose**.  
- Particionamiento automático de archivos en bloques de **4 MB**.  
- Distribución de bloques en esquema **round-robin**.  
- Persistencia de datos mediante volúmenes Docker.  
- Cliente CLI en Python para:
  - Registro/Login  
  - Subir archivos (`put`)  
  - Descargar archivos (`get`)  
  - Listar archivos (`ls`)  
  - Verificar conexión (`ping`)  

### 1.2. Aspectos no cumplidos o desarrollados de la actividad

-  No se implementó la replicación múltiple de bloques (cada bloque se almacena en un solo DataNode).  
-  No se implementó la interfaz gráfica, únicamente se dispone del **CLI**.  
-  No se realizaron pruebas en nube pública (solo entorno local con Docker).  

---

## 2. Información general de diseño de alto nivel

El diseño sigue la arquitectura Master/Worker:  
- NameNode (master): coordina, guarda metadatos, autentica usuarios.  
- DataNodes (workers): almacenan físicamente los bloques de los archivos.  
- Cliente (CLI): interfaz en Python que se comunica con el NameNode vía API REST.  

**Patrones y prácticas utilizadas:**
- Arquitectura de microservicios con contenedores.  
- Patrón de separación de responsabilidades (NameNode / DataNode / Cliente).  
- Uso de volúmenes persistentes en Docker para mantener datos.  
- Uso de **REST API con FastAPI** para comunicación entre nodos.  

---

## 3. Descripción del ambiente de desarrollo y técnico

- Lenguaje: **Python 3.10+**  
- Framework web: **FastAPI 0.95**  
- CLI: **Click 8.1**  
- Cliente HTTP: **Requests 2.31**  
- Contenedores: **Docker 25+** y **Docker Compose 2+**

### Cómo se compila y ejecuta
1. Clonar el repositorio:
   ```bash
   git clone https://github.com/usuario/griddfs-project.git
   cd griddfs-project

2. Levantar contenedores (comandos para cmd windows):
   ```bash
   docker network create gridnet
   
   docker build -t griddfs/namenode ./namenode
   
   docker build -t griddfs/datanode ./datanode
   
   docker run -d --name namenode --network gridnet -p 5000:5000 -e NODE_TYPE=namenode -e PORT=5000 -e STORAGE_PATH=/app/data -e LOG_LEVEL=INFO -v namenode_data:/app/data griddfs/namenode
   
   docker run -d --name datanode1 --network gridnet -p 5001:5000 -e NODE_ID=datanode1 -e DATANODE_URL=http://localhost:5001 -e EXTERNAL_URL=http://localhost:5001 -e NAMENODE_URL=http://namenode:5000 -e STORAGE_ROOT=/data/storage -e LOGS_PATH=/data/logs -e MAX_RETRIES=10 -v datanode1_data:/data griddfs/datanode
   
   docker run -d --name datanode2 --network gridnet -p 5002:5000 -e NODE_ID=datanode2 -e DATANODE_URL=http://localhost:5002 -e EXTERNAL_URL=http://localhost:5002 -e NAMENODE_URL=http://namenode:5000 -e STORAGE_ROOT=/data/storage -e LOGS_PATH=/data/logs -e MAX_RETRIES=10 -v datanode2_data:/data griddfs/datanode
   
   docker run -d --name datanode3 --network gridnet -p 5003:5000 -e NODE_ID=datanode3 -e DATANODE_URL=http://localhost:5003 -e EXTERNAL_URL=http://localhost:5003 -e NAMENODE_URL=http://namenode:5000 -e STORAGE_ROOT=/data/storage -e LOGS_PATH=/data/logs -e MAX_RETRIES=10 -v datanode3_data:/data griddfs/datanode
   
   
3. Uso (comandos para uso basico creando y subiendo un archivo):
   ```bash
   cd grid-client
   pip install -r requirements.txt
   python grid_cli.py register usuario contraseña
   python grid_cli.py login usuario contraseña
   python grid_cli.py datanodes -d
   python grid_cli.py put test.txt
   python grid_cli.py ls
   python grid_cli.py get test.txt descargado.txt

  ## 4. Configuración de parámetros principales
  - NameNode expuesto en puerto 5000.
  - DataNodes expuestos en puertos 5001–5004.
  - Variables de entorno en docker-compose.yml:
  - NODE_ID para identificar cada DataNode.
  - NAMENODE_URL para la comunicación interna.
  - STORAGE_ROOT para la ubicación de los bloques.

  ## 5. Información Relevante
  - El sistema fue probado con archivos de hasta 100 MB, confirmando el correcto particionamiento en 25 bloques de 4 MB cada uno.
  - Los bloques se distribuyen de manera balanceada entre los 4 DataNodes.
  - Se probó tolerancia a fallos deteniendo un DataNode y verificando que el sistema siguiera funcionando con los restantes.


