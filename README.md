#### Criação e configuração de ambiente e desenvolvimento do pipeline

##### Docker
- Instalção e configuração.

##### WSL2
- Instalação e configuração.

##### Airflow
- Criação de container no docker atraves do arquivo yaml.

##### DAG
- Objetivo da dag eh executar as task que atendem as estapas da "Ingestão".

##### Ingestão
- Camada "Raw". Gravar os dados no formato string para todas as colunas. Respeitando a integridade dos dados da origem.
- Camada "Trusted". Normalizar as nomenclaturas dos campos e ajustar os datatypes da camada "Raw".
- Camada "Delivery". Aplicar as regras de negocio da camada "Trusted".

##### SQL - Python - Spark
- Criação do script utilizando as linguagens e framework listados no titulo.
