# Formula-1-Racing-ETL-Pipeline
## Project Summary
In this Project, I have bulit an end to end ETL pipeline for analyzing Formula1 data using Azure Databricks,Pyspark,Azure Data Factory,Delta Lake and Unity Catalog,showcasing modern data lakehouse architecture and real-time analytics capabilities.
## Dataset
The data used here is obtained from the Ergast Developer API
DataSet Link:
[Formula 1 Ergast Data](https://ergast.com/mrd/)
- To know more about the Dataset :[user guide](https://ergast.com/docs/f1db_user_guide.txt)
- Below is the ER diagram:
![image](https://github.com/user-attachments/assets/a8ae5326-1f0e-4bc7-8bfc-24a256c4956c)
Tools:
- [Python](https://www.python.org/)
- [Pyspark](https://spark.apache.org/docs/latest/api/python/)
- [Azure Databricks](https://azure.microsoft.com/en-us/products/databricks/)
- [Azure Data Factory](https://azure.microsoft.com/en-us/products/data-factory)
- [Azure Data Laken Storage Gen2](https://azure.microsoft.com/en-us/solutions/data-lake/)
- [Delta Lake](https://learn.microsoft.com/en-us/azure/databricks/delta/)

## Architecture
The solution used in this project is based on the ["Modern analytics architecture with Azure Databricks"](https://learn.microsoft.com/en-us/azure/architecture/solution-ideas/articles/azure-databricks-modern-analytics-architecture) from the Azure Architecture Center:
![image](https://camo.githubusercontent.com/a95da7f94ffefc340bfb5639070c8366b6df51a0803792db201996349e811ea0/68747470733a2f2f6c6561726e2e6d6963726f736f66742e636f6d2f656e2d75732f617a7572652f6172636869746563747572652f736f6c7574696f6e2d69646561732f6d656469612f617a7572652d64617461627269636b732d6d6f6465726e2d616e616c79746963732d6172636869746563747572652d6469616772616d2e706e67)

## Project Highlights
𝗗𝗮𝘁𝗮 𝗜𝗻𝗴𝗲𝘀𝘁𝗶𝗼𝗻:
Configured Azure Data Lake Storage Gen2 to store raw Formula 1 race data and used Azure Databricks for scalable, distributed data processing.

𝗗𝗮𝘁𝗮 𝗧𝗿𝗮𝗻𝘀𝗳𝗼𝗿𝗺𝗮𝘁𝗶𝗼𝗻:
Implemented transformations using PySpark and Delta Lake, enabling schema enforcement, performance optimization, and support for both incremental and full data loads.

𝗗𝗮𝘁𝗮 𝗚𝗼𝘃𝗲𝗿𝗻𝗮𝗻𝗰𝗲:
Leveraged Unity Catalog to centralize data governance through metadata management, audit trails, and role-based access controls.

𝗘𝗧𝗟 𝗔𝘂𝘁𝗼𝗺𝗮𝘁𝗶𝗼𝗻:
Automated end-to-end data workflows using Azure Data Factory, including configuration of pipelines, triggers, and monitoring mechanisms.



