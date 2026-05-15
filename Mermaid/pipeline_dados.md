# Pipeline de Dados Estruturados — Medallion Architecture

```mermaid
flowchart TD
    API([POST /run_pipeline_dados]) --> INIT["pipeline_data.py\nget_prev_last_run()"]

    ETLTS[(gestao_db\netl_data.last_run)] --> INIT
    OPDATA[(gestao_db\nop_data\nfile_url · extract_function)] --> S1
    INIT --> S1

    S1["1/6 — validate_opdata\nbronze_validations.py\nverifica URLs e funções disponíveis"]
    S1 -->|inválido| LOGS[(gestao_db\netl_logs_dados)]
    S1 -->|válido| S2

    S2["2/6 — ingest_raw\nbronze.py\ndownload ficheiro via URL\nauto-detect: CSV / JSON / Excel / XML / ZIP"]
    S2 -->|erro download| LOGS
    S2 --> BRONZE[(MinIO\nbucket: bronze\nficheiros brutos)]

    BRONZE --> S3["3/6 — validate_bronze\nsilver_validations.py\nvalida objetos no bucket"]
    S3 -->|inválido| LOGS
    S3 -->|válido| S4

    S4["4/6 — transform\nsilver.py + silver_functions.py\nexecuta extract_function por ficheiro\nsaída: Parquet"]
    S4 --> SILVER[(MinIO\nbucket: silver\n.parquet files)]

    SILVER --> S5["5/6 — validate_silver\ngold_validations.py\nvalida estrutura dos Parquets"]
    S5 -->|inválido| LOGS
    S5 -->|válido| S6

    S6["6/6 — load\ngold.py\nmapeia Parquet → dims + fact\ncarga incremental via metadata"]
    S6 --> DW

    subgraph DW [warehouse_db]
        DI[dim_indicator]
        DL[dim_location]
        DD[dim_date]
        DR[dim_report]
        FV[fact_values]
        DI & DL & DD & DR --> FV
    end

    S6 --> UPD[(gestao_db\netl_data.last_run\natualizado)]
    S6 --> RPT[/Reports/\npipeline_data_report.py]
```
